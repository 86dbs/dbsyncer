/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.cluster;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NetUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ClusterNodeStatusEnum;
import org.dbsyncer.sdk.enums.ClusterRoleEnum;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.DeploymentService;
import org.dbsyncer.sdk.spi.LeaderLifecycleListener;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SOFAJRaft 集群控制面：入群、选主、租约与任务分配。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
@Component
@ConditionalOnProperty(prefix = "dbsyncer.cluster", name = "enabled", havingValue = "true")
public class RaftClusterService implements DeploymentService, ClusterService, ScheduledTaskJob, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ClusterStateMachine fsm = new ClusterStateMachine();

    @Resource
    private ClusterProperties properties;
    @Resource
    private ClusterNodeRepository nodeRepository;
    @Resource
    private NetworkProbe networkProbe;
    @Resource
    private MetaProfile metaProfile;
    @Resource
    @Lazy
    private ProfileComponent profileComponent;
    @Resource
    private TableGroupProfile tableGroupProfile;
    @Resource
    private ScheduledTaskService scheduledTaskService;
    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Value("${dbsyncer.storage.type:h2}")
    private String storageType;
    @Value("${server.ip:}")
    private String serverIp;
    @Value("${server.port:18686}")
    private int httpPort;

    private ClusterNode localNode;
    private Node raftNode;
    private RaftGroupService raftGroupService;
    private CliService cliService;

    @PostConstruct
    public void start() {
        Assert.isTrue(StringUtil.equalsIgnoreCase("mysql", storageType), "集群模式必须使用 MySQL 存储(dbsyncer.storage.type=mysql)");
        this.localNode = registerLocalNode();
        snowflakeIdWorker.setId(localNode.getWorkerId());
        startRaft(localNode);
        scheduledTaskService.start("cluster-heartbeat", properties.getHeartbeatIntervalMs(), this);
        logger.info("集群节点已启动, nodeId={}, raft={}", localNode.getNodeId(), localNode.getRaftPeerId());
    }

    @Override
    public void destroy() {
        if (raftGroupService != null) {
            raftGroupService.shutdown();
        }
        if (cliService != null) {
            cliService.shutdown();
        }
    }

    @Override
    public boolean isStandalone() {
        return false;
    }

    @Override
    public ClusterService getClusterService() {
        return this;
    }

    @Override
    public boolean isLeader() {
        return fsm.isLeader() || (raftNode != null && raftNode.isLeader());
    }

    @Override
    public String getLocalNodeId() {
        return localNode == null ? "" : localNode.getNodeId();
    }

    @Override
    public String getLeaderId() {
        if (raftNode == null || raftNode.getLeaderId() == null) {
            return "";
        }
        String peer = raftNode.getLeaderId().toString();
        for (ClusterNode node : listNodes()) {
            if (StringUtil.equals(peer, node.getRaftPeerId())) {
                return node.getNodeId();
            }
        }
        return peer;
    }

    @Override
    public String getLeaderHttpUrl() {
        String leaderId = getLeaderId();
        for (ClusterNode node : listNodes()) {
            if (StringUtil.equals(leaderId, node.getNodeId())) {
                return "http://" + node.getIp() + ":" + node.getHttpPort();
            }
        }
        return "";
    }

    @Override
    public ClusterRoleEnum getRole() {
        return isLeader() ? ClusterRoleEnum.LEADER : ClusterRoleEnum.FOLLOWER;
    }

    @Override
    public List<ClusterNode> listNodes() {
        return nodeRepository.listByCluster(properties.getId());
    }

    @Override
    public List<ClusterNode> listOnlineNodes() {
        return listNodes().stream()
                .filter(n -> n.getStatus() == ClusterNodeStatusEnum.ONLINE.getCode())
                .collect(Collectors.toList());
    }

    @Override
    public void assertLeaderWritable() {
        if (isLeader()) {
            return;
        }
        String url = getLeaderHttpUrl();
        String hint = StringUtil.isBlank(url) ? "请稍后重试" : "请到 Leader 操作: " + url;
        throw new SdkException("当前节点不是 Leader，" + hint);
    }

    @Override
    public boolean tryAcquireLease(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null) {
            return false;
        }
        if (isLeaseOwnedByLocal(meta)) {
            return persistLease(meta, getLocalNodeId());
        }
        if (isLeader() && isLeaseFree(meta)) {
            return persistLease(meta, getLocalNodeId());
        }
        return false;
    }

    @Override
    public boolean assignLease(String metaId, String ownerNodeId) {
        if (!isLeader()) {
            return false;
        }
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null) {
            return false;
        }
        return persistLease(meta, ownerNodeId);
    }

    @Override
    public void releaseLease(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null || !StringUtil.equals(getLocalNodeId(), meta.getLeaseOwner())) {
            return;
        }
        meta.setLeaseOwner(null);
        meta.setLeaseExpireAt(0L);
        profileComponent.editConfigModel(meta);
    }

    @Override
    public boolean hasValidLease(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        return meta != null && isLeaseOwnedByLocal(meta);
    }

    @Override
    public boolean isTableAssignedToLocal(String tableGroupId) {
        Meta detail = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (detail == null) {
            return isLeader();
        }
        if (StringUtil.isBlank(detail.getLeaseOwner()) || isLeaseFree(detail)) {
            return isLeader();
        }
        return StringUtil.equals(getLocalNodeId(), detail.getLeaseOwner());
    }

    @Override
    public void assignTableGroups(String taskId) {
        if (!isLeader()) {
            return;
        }
        List<ClusterNode> online = listOnlineNodes();
        if (CollectionUtils.isEmpty(online)) {
            return;
        }
        final int[] cursor = {0};
        tableGroupProfile.pageScanTableGroups(taskId, ConfigConstant.PAGE_SIZE, groups -> {
            for (TableGroup group : groups) {
                assignOneTable(group, online, cursor);
            }
        });
    }

    @Override
    public void assignIncrementMapping(String metaId) {
        if (!isLeader()) {
            return;
        }
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null) {
            return;
        }
        List<ClusterNode> online = listOnlineNodes();
        if (CollectionUtils.isEmpty(online)) {
            return;
        }
        if (hasLiveOwner(meta, online)) {
            persistLease(meta, meta.getLeaseOwner());
            return;
        }
        int index = Math.abs(metaId.hashCode()) % online.size();
        persistLease(meta, online.get(index).getNodeId());
    }

    @Override
    public void addLeaderListener(LeaderLifecycleListener listener) {
        fsm.addListener(listener);
    }

    @Override
    public void transferLeadership(String targetNodeId) {
        assertLeaderWritable();
        ClusterNode target = nodeRepository.get(targetNodeId);
        Assert.notNull(target, "目标节点不存在");
        PeerId peer = PeerId.parsePeer(target.getRaftPeerId());
        Status status = raftNode.transferLeadershipTo(peer);
        if (!status.isOk()) {
            throw new SdkException("切换节点失败: " + status.getErrorMsg());
        }
    }

    @Override
    public void removeNode(String nodeId) {
        assertLeaderWritable();
        ClusterNode target = nodeRepository.get(nodeId);
        Assert.notNull(target, "节点不存在");
        if (StringUtil.equals(nodeId, getLocalNodeId())) {
            throw new SdkException("不能移除当前 Leader，请先切换节点");
        }
        Configuration conf = currentConf();
        PeerId peer = PeerId.parsePeer(target.getRaftPeerId());
        Status status = cliService.removePeer(properties.getId(), conf, peer);
        if (!status.isOk()) {
            logger.warn("removePeer 失败: {}", status.getErrorMsg());
        }
        target.setStatus(ClusterNodeStatusEnum.LEAVING.getCode());
        target.setUpdateTime(System.currentTimeMillis());
        nodeRepository.save(target);
    }

    @Override
    public void run() {
        try {
            heartbeatAndProbe();
            if (isLeader()) {
                addMissingPeers();
            }
        } catch (Exception e) {
            logger.error("集群心跳任务失败: {}", e.getMessage(), e);
        }
    }

    private ClusterNode registerLocalNode() {
        String ip = NetUtil.resolveAdvertiseIp(serverIp);
        int raftPort = properties.getRaftPort() > 0 ? properties.getRaftPort() : httpPort + 100;
        String nodeId = ip + ":" + httpPort;
        ClusterNode exist = nodeRepository.get(nodeId);
        ClusterNode node = exist == null ? new ClusterNode() : exist;
        long now = System.currentTimeMillis();
        node.setNodeId(nodeId);
        node.setClusterId(properties.getId());
        node.setName(nodeId);
        node.setIp(ip);
        node.setHttpPort(httpPort);
        node.setRaftPort(raftPort);
        node.setRaftPeerId(ip + ":" + raftPort);
        node.setWorkerId(exist == null ? allocateWorkerId() : exist.getWorkerId());
        node.setRole(ClusterRoleEnum.FOLLOWER.getCode());
        node.setStatus(ClusterNodeStatusEnum.JOINING.getCode());
        node.setNetworkOk(1);
        node.setStartTime(now);
        node.setLastHeartbeatTime(now);
        node.setUpdateTime(now);
        if (exist == null) {
            node.setCreateTime(now);
        }
        nodeRepository.save(node);
        return node;
    }

    private int allocateWorkerId() {
        List<ClusterNode> nodes = nodeRepository.listByCluster(properties.getId());
        boolean[] used = new boolean[32];
        for (ClusterNode node : nodes) {
            if (node.getWorkerId() >= 0 && node.getWorkerId() < 32) {
                used[node.getWorkerId()] = true;
            }
        }
        for (int i = 0; i < 32; i++) {
            if (!used[i]) {
                return i;
            }
        }
        throw new SdkException("集群节点超过 32 台，无法分配 worker.id");
    }

    private void startRaft(ClusterNode node) {
        String lockName = "dbsyncer_cluster_" + properties.getId();
        boolean locked = nodeRepository.tryLock(lockName, 10);
        try {
            List<ClusterNode> livePeers = listLivePeers(node);
            // 表里残留 ONLINE 但 Raft 端口已死的旧行不能挡住首节点 bootstrap
            boolean bootstrap = locked && livePeers.isEmpty();
            PeerId serverId = PeerId.parsePeer(node.getRaftPeerId());
            NodeOptions options = buildNodeOptions(node, bootstrap);
            RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
            raftGroupService = new RaftGroupService(properties.getId(), serverId, options, rpcServer);
            raftNode = raftGroupService.start();
            cliService = RaftServiceFactory.createAndInitCliService(new CliOptions());
            logger.info("Raft 已启动, bootstrap={}, livePeers={}", bootstrap, livePeers.size());
            if (!bootstrap) {
                requestAddPeer(node, livePeers);
            }
        } finally {
            if (locked) {
                nodeRepository.releaseLock(lockName);
            }
        }
    }

    private NodeOptions buildNodeOptions(ClusterNode node, boolean bootstrap) {
        NodeOptions options = new NodeOptions();
        options.setElectionTimeoutMs(1000);
        options.setDisableCli(false);
        options.setFsm(fsm);
        // 目录含通告 IP，避免 127.0.0.1 与局域网 IP 复用同一份 Raft 元数据导致无法选主
        String dataPath = "./data/raft-" + StringUtil.replace(node.getIp(), ".", "_") + "-" + node.getHttpPort();
        new File(dataPath + "/log").mkdirs();
        new File(dataPath + "/meta").mkdirs();
        new File(dataPath + "/snapshot").mkdirs();
        options.setLogUri(dataPath + "/log");
        options.setRaftMetaUri(dataPath + "/meta");
        options.setSnapshotUri(dataPath + "/snapshot");
        logger.info("Raft 数据目录={}, peer={}", dataPath, node.getRaftPeerId());
        if (bootstrap) {
            Configuration conf = new Configuration();
            conf.addPeer(PeerId.parsePeer(node.getRaftPeerId()));
            options.setInitialConf(conf);
        }
        return options;
    }

    private List<ClusterNode> listLivePeers(ClusterNode self) {
        List<ClusterNode> result = new ArrayList<>();
        for (ClusterNode node : nodeRepository.listByCluster(properties.getId())) {
            if (node == null || StringUtil.equals(self.getNodeId(), node.getNodeId())) {
                continue;
            }
            if (networkProbe.pingTcp(node.getIp(), node.getRaftPort())) {
                result.add(node);
            }
        }
        return result;
    }

    private void requestAddPeer(ClusterNode self, List<ClusterNode> others) {
        if (self == null || cliService == null || CollectionUtils.isEmpty(others)) {
            return;
        }
        Configuration conf = toConf(others);
        PeerId me = PeerId.parsePeer(self.getRaftPeerId());
        if (isPeerInGroup(me, conf)) {
            return;
        }
        Status status = cliService.addPeer(properties.getId(), conf, me);
        if (!status.isOk() && !isPeerAlreadyExists(status)) {
            logger.warn("向 Leader 申请 addPeer 失败: {}", status.getErrorMsg());
        }
    }

    private void heartbeatAndProbe() {
        if (localNode == null) {
            return;
        }
        long now = System.currentTimeMillis();
        localNode.setLastHeartbeatTime(now);
        localNode.setUpdateTime(now);
        localNode.setRole(getRole().getCode());
        localNode.setTerm(fsm.getLeaderTerm());
        // 尚未进入 Raft 配置的节点保持 JOINING，供 Leader addPeer
        localNode.setStatus(isLeader() || isLocalInConf()
                ? ClusterNodeStatusEnum.ONLINE.getCode()
                : ClusterNodeStatusEnum.JOINING.getCode());
        nodeRepository.save(localNode);
        if (isLeader()) {
            markUnreachable(now);
        }
    }

    private void markUnreachable(long now) {
        String leaderId = getLeaderId();
        Configuration conf = currentConf();
        for (ClusterNode node : listNodes()) {
            if (node == null || StringUtil.equals(node.getNodeId(), getLocalNodeId())) {
                continue;
            }
            if (node.getStatus() == ClusterNodeStatusEnum.JOINING.getCode()
                    || node.getStatus() == ClusterNodeStatusEnum.LEAVING.getCode()) {
                continue;
            }
            boolean reachable = networkProbe.isReachable(node.getIp(), node.getHttpPort(), node.getRaftPort());
            boolean timeout = now - node.getLastHeartbeatTime() > properties.getHeartbeatTimeoutMs();
            int status = reachable && !timeout ? ClusterNodeStatusEnum.ONLINE.getCode()
                    : ClusterNodeStatusEnum.UNREACHABLE.getCode();
            node.setNetworkOk(reachable ? 1 : 0);
            node.setStatus(status);
            node.setUpdateTime(now);
            if (!StringUtil.equals(leaderId, node.getNodeId())) {
                node.setRole(ClusterRoleEnum.FOLLOWER.getCode());
            }
            if (timeout && !networkProbe.pingTcp(node.getIp(), node.getRaftPort())) {
                evictUnreachablePeer(node, conf);
            }
            nodeRepository.save(node);
        }
    }

    private void evictUnreachablePeer(ClusterNode node, Configuration conf) {
        if (node == null || conf == null || cliService == null) {
            return;
        }
        PeerId peer = PeerId.parsePeer(node.getRaftPeerId());
        if (!conf.contains(peer)) {
            return;
        }
        Status status = cliService.removePeer(properties.getId(), conf, peer);
        if (status.isOk()) {
            conf.removePeer(peer);
            logger.info("节点不可达，已从 Raft 配置移除: {}", node.getRaftPeerId());
            return;
        }
        logger.warn("移除不可达节点 {} 失败: {}", node.getRaftPeerId(), status.getErrorMsg());
    }

    private void addMissingPeers() {
        Configuration conf = currentConf();
        for (ClusterNode node : listNodes()) {
            if (node == null || StringUtil.equals(node.getNodeId(), getLocalNodeId())) {
                continue;
            }
            PeerId peer = PeerId.parsePeer(node.getRaftPeerId());
            if (conf.contains(peer) || isPeerInGroup(peer, conf)) {
                continue;
            }
            if (!networkProbe.pingTcp(node.getIp(), node.getRaftPort())) {
                continue;
            }
            Status status = cliService.addPeer(properties.getId(), conf, peer);
            if (status.isOk() || isPeerAlreadyExists(status)) {
                node.setStatus(ClusterNodeStatusEnum.ONLINE.getCode());
                node.setUpdateTime(System.currentTimeMillis());
                nodeRepository.save(node);
                conf.addPeer(peer);
                if (status.isOk()) {
                    logger.info("已将节点加入 Raft: {}", node.getRaftPeerId());
                }
            } else {
                logger.warn("addPeer {} 失败: {}", node.getRaftPeerId(), status.getErrorMsg());
            }
        }
    }

    private boolean isLocalInConf() {
        if (localNode == null) {
            return false;
        }
        if (isLeader()) {
            return true;
        }
        PeerId me = PeerId.parsePeer(localNode.getRaftPeerId());
        if (fsm.getCommittedConf().contains(me)) {
            return true;
        }
        // 重启后 onConfigurationCommitted 不一定立刻回调，已收到 Leader 心跳即视为已入群
        return raftNode != null && raftNode.getLeaderId() != null;
    }

    private Configuration toConf(List<ClusterNode> nodes) {
        Configuration conf = new Configuration();
        if (CollectionUtils.isEmpty(nodes)) {
            return conf;
        }
        for (ClusterNode node : nodes) {
            if (node != null) {
                conf.addPeer(PeerId.parsePeer(node.getRaftPeerId()));
            }
        }
        return conf;
    }

    private boolean isPeerInGroup(PeerId peer, Configuration conf) {
        if (peer == null || conf == null || conf.isEmpty() || cliService == null) {
            return false;
        }
        try {
            List<PeerId> peers = cliService.getPeers(properties.getId(), conf);
            return peers != null && peers.contains(peer);
        } catch (Exception e) {
            logger.debug("查询 Raft 成员失败: {}", e.getMessage());
            return false;
        }
    }

    private boolean isPeerAlreadyExists(Status status) {
        return status != null && StringUtil.contains(status.getErrorMsg(), "already exists");
    }

    private void assignOneTable(TableGroup group, List<ClusterNode> online, int[] cursor) {
        if (group == null || StringUtil.isBlank(group.getId())) {
            return;
        }
        Meta detail = metaProfile.getMetaByTaskId(group.getId(), TaskLevelEnum.TASK_DETAIL);
        if (detail == null || detail.getState() == CommonTaskStatusEnum.DONE.getCode()) {
            return;
        }
        if (hasLiveOwner(detail, online)) {
            persistLease(detail, detail.getLeaseOwner());
            return;
        }
        String owner = online.get(cursor[0] % online.size()).getNodeId();
        cursor[0]++;
        persistLease(detail, owner);
    }

    private boolean persistLease(Meta meta, String ownerNodeId) {
        meta.setEpoch(meta.getEpoch() + 1);
        meta.setLeaseOwner(ownerNodeId);
        meta.setLeaseExpireAt(System.currentTimeMillis() + properties.getLeaseTtlMs());
        profileComponent.editConfigModel(meta);
        return true;
    }

    private boolean isLeaseOwnedByLocal(Meta meta) {
        return StringUtil.equals(getLocalNodeId(), meta.getLeaseOwner())
                && meta.getLeaseExpireAt() > System.currentTimeMillis();
    }

    private boolean isLeaseFree(Meta meta) {
        return StringUtil.isBlank(meta.getLeaseOwner()) || meta.getLeaseExpireAt() <= System.currentTimeMillis();
    }

    private boolean hasLiveOwner(Meta meta, List<ClusterNode> online) {
        if (isLeaseFree(meta)) {
            return false;
        }
        for (ClusterNode node : online) {
            if (StringUtil.equals(meta.getLeaseOwner(), node.getNodeId())) {
                return true;
            }
        }
        return false;
    }

    private Configuration currentConf() {
        Configuration conf = new Configuration();
        // listPeers 仅 Leader 可用，Follower 调用会抛 Not leader
        if (raftNode != null && isLeader()) {
            try {
                List<PeerId> peers = raftNode.listPeers();
                if (peers != null) {
                    for (PeerId peer : peers) {
                        conf.addPeer(peer);
                    }
                }
            } catch (IllegalStateException e) {
                logger.debug("读取 Raft 成员失败: {}", e.getMessage());
            }
        }
        if (conf.isEmpty()) {
            conf = fsm.getCommittedConf();
        }
        if (conf.isEmpty() && localNode != null) {
            conf.addPeer(PeerId.parsePeer(localNode.getRaftPeerId()));
        }
        return conf;
    }
}

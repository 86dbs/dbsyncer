/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.cluster;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.storage.SqlQuery;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 集群节点表访问。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
@Component
@ConditionalOnProperty(prefix = "dbsyncer.cluster", name = "enabled", havingValue = "true")
public class ClusterNodeRepository {

    @Resource
    private StorageService storageService;

    /**
     * 按节点业务 ID 查询。
     *
     * @param nodeId 节点 ID，{ip}:{httpPort}
     * @return 节点，不存在为 null
     */
    public ClusterNode get(String nodeId) {
        Query query = new Query(1, 1);
        query.setType(StorageEnum.CLUSTER_NODE);
        query.addFilter(ConfigConstant.CLUSTER_NODE_ID, nodeId);
        List<ClusterNode> list = queryNodes(query);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    /**
     * 同集群全部节点。
     *
     * @param clusterId 集群 ID
     * @return 节点列表
     */
    public List<ClusterNode> listByCluster(String clusterId) {
        Query query = new Query(1, 1000);
        query.setType(StorageEnum.CLUSTER_NODE);
        query.addFilter(ConfigConstant.CLUSTER_CLUSTER_ID, clusterId);
        return queryNodes(query);
    }

    /**
     * 新增或覆盖。首次插入不写 ID，由数据库自增。
     *
     * @param node 节点
     */
    public void save(ClusterNode node) {
        ClusterNode exist = get(node.getNodeId());
        Map<String, Object> params = toMap(node);
        if (exist == null) {
            params.remove(ConfigConstant.CONFIG_MODEL_ID);
            storageService.add(StorageEnum.CLUSTER_NODE, params);
            ClusterNode created = get(node.getNodeId());
            if (created != null) {
                node.setId(created.getId());
            }
            return;
        }
        node.setId(exist.getId());
        params.put(ConfigConstant.CONFIG_MODEL_ID, exist.getId());
        storageService.edit(StorageEnum.CLUSTER_NODE, params);
    }

    /**
     * MySQL GET_LOCK。
     *
     * @param lockName 锁名
     * @param timeout  秒
     * @return true 持有锁
     */
    public boolean tryLock(String lockName, int timeout) {
        List<Map<String, Object>> rows = storageService.queryList(
                SqlQuery.of("SELECT GET_LOCK(?, ?) AS locked", lockName, timeout));
        if (CollectionUtils.isEmpty(rows)) {
            return false;
        }
        Object locked = rows.get(0).get("locked");
        if (locked == null) {
            locked = rows.get(0).values().iterator().next();
        }
        return locked != null && !"0".equals(String.valueOf(locked));
    }

    /**
     * 释放 GET_LOCK。
     *
     * @param lockName 锁名
     */
    public void releaseLock(String lockName) {
        storageService.queryList(SqlQuery.of("SELECT RELEASE_LOCK(?) AS unlocked", lockName));
    }

    private List<ClusterNode> queryNodes(Query query) {
        org.dbsyncer.common.model.Paging paging = storageService.query(query);
        List<ClusterNode> result = new ArrayList<>();
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return result;
        }
        for (Object item : paging.getData()) {
            if (item instanceof Map) {
                ClusterNode node = toNode((Map) item);
                if (node != null) {
                    result.add(node);
                }
            }
        }
        return result;
    }

    private ClusterNode toNode(Map item) {
        Object id = item.get(ConfigConstant.CONFIG_MODEL_ID);
        if (id != null && !(id instanceof String)) {
            item.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(id));
        }
        return JsonUtil.mapToObj(item, ClusterNode.class);
    }

    private Map<String, Object> toMap(ClusterNode node) {
        Map<String, Object> params = new HashMap<>();
        if (StringUtil.isNotBlank(node.getId())) {
            params.put(ConfigConstant.CONFIG_MODEL_ID, node.getId());
        }
        params.put(ConfigConstant.CLUSTER_NODE_ID, node.getNodeId());
        params.put(ConfigConstant.CLUSTER_CLUSTER_ID, node.getClusterId());
        params.put(ConfigConstant.CONFIG_MODEL_NAME, node.getName());
        params.put(ConfigConstant.CLUSTER_IP, node.getIp());
        params.put(ConfigConstant.CLUSTER_HTTP_PORT, node.getHttpPort());
        params.put(ConfigConstant.CLUSTER_RAFT_PORT, node.getRaftPort());
        params.put(ConfigConstant.CLUSTER_RAFT_PEER_ID, node.getRaftPeerId());
        params.put(ConfigConstant.CLUSTER_WORKER_ID, node.getWorkerId());
        params.put(ConfigConstant.CLUSTER_ROLE, node.getRole());
        params.put(ConfigConstant.CLUSTER_STATUS, node.getStatus());
        params.put(ConfigConstant.CLUSTER_NETWORK_OK, node.getNetworkOk());
        params.put(ConfigConstant.CLUSTER_TERM, node.getTerm());
        params.put(ConfigConstant.CLUSTER_LAST_HEARTBEAT_TIME, node.getLastHeartbeatTime());
        params.put(ConfigConstant.CLUSTER_START_TIME, node.getStartTime());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, node.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, node.getUpdateTime());
        return params;
    }
}

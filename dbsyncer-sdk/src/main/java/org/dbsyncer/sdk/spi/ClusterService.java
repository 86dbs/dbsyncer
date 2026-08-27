/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.ClusterRoleEnum;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.model.WorkItemAssignment;
import org.dbsyncer.sdk.model.WorkItemIds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 集群控制服务：节点角色、工作项派工与围栏。单机实现恒为 Leader。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface ClusterService {

    /**
     * 当前运行时是否启用集群能力（授权、开关、存储等门控）。默认 false。
     *
     * @param licenseService 许可证服务，可空
     * @param clusterEnabled 配置开关 {@code dbsyncer.cluster.enabled}
     * @param storageType    存储类型 {@code dbsyncer.storage.type}
     * @return true 使用本实现作为集群控制面
     */
    default boolean isClusterRuntime(LicenseService licenseService, boolean clusterEnabled, String storageType) {
        return false;
    }

    /**
     * 是否单机部署。
     *
     * @return true 单机
     */
    boolean isStandalone();

    /**
     * 本节点是否 Leader。
     *
     * @return true Leader
     */
    boolean isLeader();

    /**
     * 本节点 ID。
     *
     * @return 节点 ID
     */
    String getLocalNodeId();

    /**
     * 当前 Leader 节点 ID，未知时为空。
     *
     * @return Leader ID
     */
    String getLeaderId();

    /**
     * Leader 的 HTTP 访问地址，供 Follower 提示跳转。
     *
     * @return 如 {@code http(s)://ip:port}（协议跟随 {@code server.ssl.enabled}），未知时为空
     */
    String getLeaderHttpUrl();

    /**
     * 本节点角色。
     *
     * @return 角色
     */
    ClusterRoleEnum getRole();

    /**
     * 集群节点列表。
     *
     * @return 节点，单机为空列表
     */
    List<ClusterNode> listNodes();

    /**
     * 分页查询集群节点。顺序固定为创建时间升序、节点 ID 升序（不按心跳更新时间排序）。
     * <p>默认基于 {@link #listNodes()} 内存分页；存储侧可覆盖为库表分页。
     *
     * @param pageNum  页码（从 1 起）
     * @param pageSize 每页条数
     * @return 分页结果
     */
    default Paging<ClusterNode> queryNodes(int pageNum, int pageSize) {
        int safePageNum = Math.max(1, pageNum);
        int safePageSize = Math.max(1, pageSize);
        List<ClusterNode> source = listNodes();
        List<ClusterNode> all = new ArrayList<>(source == null ? Collections.emptyList() : source);
        all.sort(Comparator
                .comparingLong(ClusterNode::getCreateTime)
                .thenComparing(n -> StringUtil.getIfBlank(n.getNodeId(), StringUtil.EMPTY), String::compareTo));
        Paging<ClusterNode> paging = new Paging<>(safePageNum, safePageSize);
        paging.setTotal(all.size());
        int offset = (safePageNum - 1) * safePageSize;
        if (offset >= all.size()) {
            paging.setData(Collections.emptyList());
            return paging;
        }
        paging.setData(all.stream().skip(offset).limit(safePageSize).collect(Collectors.toList()));
        return paging;
    }

    /**
     * 在线节点。
     *
     * @return 在线节点
     */
    default List<ClusterNode> listOnlineNodes() {
        return Collections.emptyList();
    }

    /**
     * 非 Leader 写配置时抛错。
     *
     * @throws SdkException 当前不是 Leader
     */
    void assertLeaderWritable();

    /**
     * 表级工作是否分配给本节点。单机恒 true。
     *
     * @param tableGroupId 表映射 ID
     * @return true 应在本节点执行
     */
    default boolean isTableAssignedToLocal(String tableGroupId) {
        return true;
    }

    /**
     * 拉取指定节点当前 Assignment（仅 Leader 有权威视图；Follower 本地实现可转发）。
     *
     * @param nodeId 节点 ID
     * @return 分配列表
     */
    default List<WorkItemAssignment> listAssignments(String nodeId) {
        return Collections.emptyList();
    }

    /**
     * 写目标库 / 刷 Meta 前围栏：本节点仍持有该工作项的最新 generation。
     *
     * @param itemId 工作项 ID（整表为 tableGroupId，拆分项见 {@link WorkItemIds}）
     * @return true 允许产生副作用
     */
    default boolean assertWritable(String itemId) {
        return true;
    }

    /**
     * 本节点当前持有的 item generation；未持有时为 0。
     *
     * @param itemId 工作项 ID
     * @return generation
     */
    default long getLocalGeneration(String itemId) {
        return 0L;
    }

    /**
     * 本节点当前 Assignment 快照（含整表与拆分工作项）。
     *
     * @return 分配列表
     */
    default List<WorkItemAssignment> listLocalAssignments() {
        return Collections.emptyList();
    }

    /**
     * 本节点应执行的表内工作项 ID；单机恒为整表。
     *
     * @param tableGroupId 表映射 ID
     * @return itemId 列表
     */
    default List<String> resolveLocalWorkItems(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return Collections.emptyList();
        }
        if (isStandalone()) {
            return Collections.singletonList(tableGroupId);
        }
        List<String> items = new ArrayList<>();
        for (WorkItemAssignment assignment : listLocalAssignments()) {
            if (assignment != null && WorkItemIds.belongsToTable(assignment.getItemId(), tableGroupId)) {
                items.add(assignment.getItemId());
            }
        }
        return items;
    }

    /**
     * Leader 全部 Assignment 快照（排障 / 工作项详情）。
     *
     * @return 分配列表；非 Leader 为空
     */
    default List<WorkItemAssignment> listAllAssignments() {
        return Collections.emptyList();
    }

    /**
     * 启动 Mapping 前准备派工，并返回本机是否应拉起 Puller。
     * <p>默认恒 true（单机）。集群实现：Leader 按全量/增量派工；全量阶段各节点可拉起，
     * 增量阶段仅派工到本机的节点返回 true。
     *
     * @param mappingId      驱动/Mapping ID
     * @param model          同步方式（{@link org.dbsyncer.sdk.enums.ModelEnum} code）
     * @param incrementPhase 是否已进入增量阶段（全量+增量切换后）
     * @return true 本机应执行 puller.start
     */
    default boolean prepareMappingStart(String mappingId, String model, boolean incrementPhase) {
        return true;
    }

    /**
     * Leader 将未完成表派工到在线节点。单机空操作。
     *
     * @param taskId 任务/Mapping ID
     */
    default void assignTableGroups(String taskId) {
    }

    /**
     * Leader 将整增量 Mapping 派工到一台在线节点（不拆表）。单机空操作。
     *
     * @param mappingId 驱动/Mapping ID
     */
    default void assignIncrementMapping(String mappingId) {
    }

    /**
     * 增量 Mapping 是否分配给本节点执行。单机恒 true。
     *
     * @param mappingId 驱动/Mapping ID
     * @return true 应在本节点拉起 Listener
     */
    default boolean isIncrementAssignedToLocal(String mappingId) {
        return true;
    }

    /**
     * Leader 清除某任务的全部派工（停止/完成/非 RUNNING 时调用）。单机空操作。
     *
     * @param taskId 任务/Mapping ID
     */
    default void clearTaskAssignments(String taskId) {
    }

    /**
     * 任务下全部 TableGroup 是否已完成。单机恒 true。
     *
     * @param taskId 任务/Mapping ID
     * @return true 全部完成
     */
    default boolean areAllTablesDone(String taskId) {
        return true;
    }

    /**
     * 注册 Leader 升降监听。
     *
     * @param listener 监听器
     */
    void addLeaderListener(LeaderLifecycleListener listener);

    /**
     * 手动转让 Leader。
     *
     * @param targetNodeId 目标节点 ID
     */
    default void transferLeadership(String targetNodeId) {
        throw new SdkException("单机模式不支持切换节点");
    }

    /**
     * 从集群移除节点。
     *
     * @param nodeId 节点 ID
     */
    default void removeNode(String nodeId) {
        throw new SdkException("单机模式不支持移除节点");
    }
}

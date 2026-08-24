/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.ClusterRoleEnum;
import org.dbsyncer.sdk.model.ClusterNode;

import java.util.Collections;
import java.util.List;

/**
 * 集群控制服务
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface ClusterService {

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
     * 写目标库 / 刷 Meta 前围栏：本节点仍持有该 item 的最新 generation。
     *
     * @param itemId 工作项 ID（Phase1 为 tableGroupId）
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
     * Leader 将未完成表粘滞派工到在线节点。单机空操作。
     *
     * @param taskId 任务/Mapping ID
     */
    default void assignTableGroups(String taskId) {
    }

    /**
     * Leader 将整增量 Mapping 粘滞派工到一台在线节点（不拆表）。单机空操作。
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

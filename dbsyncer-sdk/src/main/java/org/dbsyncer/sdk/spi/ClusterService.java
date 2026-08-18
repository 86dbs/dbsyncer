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
 * 集群控制面：选主、租约、任务分配。单机实现恒为 Leader。
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
     * @return 如 http://ip:port，未知时为空
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
     * 抢占任务租约（本节点为 owner）。
     *
     * @param metaId Meta 主键
     * @return true 持有租约
     */
    boolean tryAcquireLease(String metaId);

    /**
     * 将租约分配给指定节点（仅 Leader）。
     *
     * @param metaId       Meta 主键
     * @param ownerNodeId  目标节点
     * @return true 分配成功
     */
    default boolean assignLease(String metaId, String ownerNodeId) {
        return tryAcquireLease(metaId);
    }

    /**
     * 释放本节点持有的租约。
     *
     * @param metaId Meta 主键
     */
    void releaseLease(String metaId);

    /**
     * 本节点是否持有未过期租约。
     *
     * @param metaId Meta 主键
     * @return true 持有
     */
    boolean hasValidLease(String metaId);

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
     * Leader 将未完成表均分到在线节点。单机空操作。
     *
     * @param taskId 任务/Mapping ID
     */
    default void assignTableGroups(String taskId) {
    }

    /**
     * Leader 将增量 Mapping 分配到一台在线节点。单机空操作。
     *
     * @param metaId 任务级 Meta ID
     */
    default void assignIncrementMapping(String metaId) {
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

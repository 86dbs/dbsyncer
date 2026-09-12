/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.ClusterNode;

import java.util.Map;

/**
 * 集群服务：节点管理与任务级调度。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface ClusterService {

    default void init() {
    }

    default boolean isStandalone() {
        return true;
    }

    default String getLocalNodeId() {
        return "standalone";
    }

    default ClusterNode getNode(String nodeId) {
        return null;
    }

    default Paging<ClusterNode> query(Map<String, String> params) {
        return null;
    }

    default void updateNodeName(String nodeId, String name) {
    }

    default void removeNode(String nodeId) {
    }

    default void bindTaskRunner(TaskRunner runner) {
    }

    default void start(String taskId, String model, boolean autoRecovery) {
    }

    default void stop(String taskId) {
    }

    /**
     * 接收集群节点通知消息，按 type/event 路由。
     *
     * @param message JSON 消息正文
     */
    default void receiveMessage(String message) {
    }

    /**
     * Leader 保护期剩余秒数。
     *
     * @return 剩余秒数；非保护期为 0
     */
    default long getGracePeriod() {
        return 0L;
    }

    /**
     * 结束保护期并通知集群各节点；随后恢复离线/未分配任务。
     */
    default void forceExpireLeaderGracePeriod() {
    }

    /**
     * 节点间通知：仅结束本机保护期门控（不扇出、不调度）。
     */
    default void acceptForceExpireLeaderGracePeriod() {
    }

}

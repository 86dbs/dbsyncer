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

    default Object handleMessage(String message) {
        return null;
    }

    default long forceExpireGracePeriod() {
        return 0L;
    }

}

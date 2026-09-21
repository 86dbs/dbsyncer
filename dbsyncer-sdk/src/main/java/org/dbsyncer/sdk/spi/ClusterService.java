/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Task;
import org.dbsyncer.sdk.schema.SchemaResolver;

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

    default void start(ConfigModel configModel, boolean autoRecovery) {
    }

    default void stop(String taskId) {
    }

    default Object handleMessage(String message) {
        return null;
    }

    default long forceExpireGracePeriod() {
        return 0L;
    }

    /**
     * 全量页级结果落库。
     *
     * @param task                 运行态（分片执行时 id 为分片计划 ID）
     * @param result               本批写结果
     * @param targetSchemaResolver 目标库类型解析
     * @param targetFieldMap       目标字段
     */
    void flush(Task task, Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap);
}

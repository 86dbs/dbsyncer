/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.message.CommonMessage;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PluginFile;
import org.dbsyncer.sdk.model.Task;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.Collections;
import java.util.List;
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

    /**
     * 停止任务。
     *
     * @param taskId   任务 ID
     * @param taskType 任务类型
     */
    default void stop(String taskId, CommonTaskTypeEnum taskType) {
    }

    default Object receiveMessage(String message) {
        return null;
    }

    default void pushMessage(CommonMessage message) {
    }

    /**
     * 将本机插件文件同步到其他节点。
     *
     * @param fileName 插件文件名
     */
    default void publishPlugin(String fileName) {
    }

    /**
     * 从 Leader 对齐本机插件。
     */
    default void syncPluginsFromLeader() {
    }

    /**
     * 本机插件目录中的 JAR 清单。
     *
     * @return 文件名、大小与摘要
     */
    default List<PluginFile> listPluginFiles() {
        return Collections.emptyList();
    }

    /**
     * 读取本机插件文件。
     *
     * @param fileName 文件名
     * @return 文件字节；不存在时返回 null
     */
    default byte[] readPluginFile(String fileName) {
        return null;
    }

    /**
     * 接收其他节点推送的插件文件并加载。
     *
     * @param fileName 文件名
     * @param content  文件字节
     * @param relay    为 true 时接收节点继续分发给其他节点
     */
    default void acceptPluginFile(String fileName, byte[] content, boolean relay) {
        throw new UnsupportedOperationException("当前部署不接收插件文件");
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

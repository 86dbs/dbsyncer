/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.model.ClusterNode;

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

    /**
     * 获取节点
     *
     * @param nodeId 节点 ID
     * @return 节点，不存在返回 null
     */
    default ClusterNode getNode(String nodeId) {
        return null;
    }

    /**
     * 分页查询节点
     *
     * @param pageNum  页码，从 1 开始
     * @param pageSize 每页条数
     * @return 节点分页
     */
    default Paging<ClusterNode> queryNodes(int pageNum, int pageSize) {
        return new Paging<>(pageNum, pageSize);
    }

    /**
     * 修改节点名称
     *
     * @param nodeId 节点 ID
     * @param name   展示名称
     */
    default void updateNodeName(String nodeId, String name) {
        throw new SdkException("单机模式不支持修改节点名称");
    }

    /**
     * 移除离线节点
     *
     * @param nodeId 节点 ID
     */
    default void removeNode(String nodeId) {
        throw new SdkException("单机模式不支持移除节点");
    }

    /**
     * 绑定本机任务执行器（由 Manager 在启动后注册）。
     *
     * @param runner 本机拉起/停止 Puller
     */
    default void bindTaskRunner(TaskRunner runner) {
    }

    /**
     * 启动任务：单机本机执行；集群选 Scheduler 后本机执行或通知远端。
     *
     * @param taskId       任务 ID（Mapping ID）
     * @param model        同步方式
     * @param autoRecovery 是否重启自动恢复
     */
    default void start(String taskId, String model, boolean autoRecovery) {
        throw new SdkException("未绑定本机任务执行器");
    }

    /**
     * 停止任务：单机本机停止；集群清调度并通知原 Scheduler 停止。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    default void stop(String taskId) {
        throw new SdkException("未绑定本机任务执行器");
    }

    /**
     * 内部拉起执行器（校验本机仍为调度节点）。禁止再走用户启动链。
     *
     * @param taskId       任务 ID
     * @param autoRecovery 是否重启自动恢复
     * @return true 已拉起；false 本机不是调度节点或未绑定执行器
     */
    default boolean execute(String taskId, boolean autoRecovery) {
        return false;
    }

    /**
     * 内部停止执行器。
     *
     * @param taskId 任务 ID
     */
    default void stopExecute(String taskId) {
    }

    /**
     * 本机是否为该任务的执行者。
     *
     * @param taskId 任务 ID
     * @return 单机恒为 true
     */
    default boolean isTaskAssignedToLocal(String taskId) {
        return true;
    }

    /**
     * 任务恢复保护期剩余秒数。
     */
    default long getLeaderProtectRemainSeconds() {
        return 0L;
    }

    /**
     * 结束保护期并立即恢复离线/未分配任务。
     */
    default void recoverOfflineTasks() {
    }

    /**
     * 尝试由控制面接管批处理全量编排。
     * <p>单机或不接管时返回 false，调用方继续走本机整表执行器。
     *
     * @param taskId 任务 ID（Mapping ID）
     * @param model  同步方式
     * @return true 已接管
     */
    default boolean tryStartShardOrchestration(String taskId, String model) {
        return false;
    }

    /**
     * 停止批处理全量编排并清理在途计划。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    default void stopShardOrchestration(String taskId) {
    }

    /**
     * 本机是否正在对该任务做批处理全量编排。
     *
     * @param taskId 任务 ID（Mapping ID）
     * @return true 编排进行中
     */
    default boolean isShardOrchestrationActive(String taskId) {
        return false;
    }

}

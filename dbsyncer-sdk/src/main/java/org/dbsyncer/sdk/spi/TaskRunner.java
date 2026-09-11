/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * 本机任务执行器：由 Manager 绑定，供集群/单机控制面拉起或停止 Puller。
 * <p>内部通知路径只应调用本接口，禁止再走用户启动链。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-07
 */
public interface TaskRunner {

    /**
     * 本机拉起任务执行器。
     *
     * @param taskId       任务 ID（Mapping ID）
     * @param autoRecovery 是否重启自动恢复
     */
    void start(String taskId, boolean autoRecovery);

    /**
     * 本机停止任务执行器。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    void stop(String taskId);

    /**
     * 按任务恢复源/目标连接（集群接回或启动前调用；单机默认可空操作）。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    default void restoreConnector(String taskId) {
    }

    /**
     * 释放任务连接（集群停止或任务结束后调用；单机默认可空操作）。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    default void releaseConnector(String taskId) {
    }

    /**
     * 全量+增量进入批处理全量前：可恢复则跳过，否则捕获增量位点。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    default void prepareFullIncrement(String taskId) {
    }

    /**
     * 批处理全量完成后切换到增量阶段并启动增量。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    default void switchToIncrementAfterFull(String taskId) {
    }

    /**
     * 纯全量批处理收口（发布关闭事件，驱动 Meta 就绪与连接回收）。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    default void completeBatchFull(String taskId) {
    }
}

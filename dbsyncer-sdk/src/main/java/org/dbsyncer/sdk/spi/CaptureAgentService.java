/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * 采集任务启停扩展点。配置读写在开源；启动与停止由专业版实现。
 * <p>支持 {@code captureLocal} / {@code captureRemote}。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
public interface CaptureAgentService {

    /**
     * 初始化（专业版装载运行时依赖）。
     */
    default void init() {
    }

    /**
     * 运行时是否可用（专业版实现返回 true）。
     *
     * @return 是否可启停
     */
    default boolean isAvailable() {
        return false;
    }

    /**
     * 启动采集任务。
     *
     * @param id 任务主键（{@code dbsyncer_task.ID}）
     */
    default void start(String id) {
        throw new UnsupportedOperationException("采集任务启停仅专业版可用");
    }

    /**
     * 停止采集任务。
     *
     * @param id 任务主键
     */
    default void stop(String id) {
        throw new UnsupportedOperationException("采集任务启停仅专业版可用");
    }
}

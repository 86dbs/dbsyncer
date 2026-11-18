/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 采集任务启停扩展点；无 SPI 实现时由 Manager 注入空实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
public interface CaptureAgentService {

    default void init() {
    }

    default boolean isAvailable() {
        return false;
    }

    /**
     * 程序冷启动恢复：预加载完成后由远端向本地上报最小位点（运行中重连/手动启停不调用）。
     */
    default void minCheckpoint() {
    }

    default void start(String id) {
    }

    default void stop(String id) {
    }

    default void bindSchema(String captureTaskId, String mappingId, String schema) {
    }

    default void unbindSchema(String captureTaskId, String mappingId) {
    }

    /**
     * 远端采集运行态摘要。约定键：{@code queueSize}、{@code queueCapacity}、{@code checkpoint}。
     */
    default Map<String, Object> getRemoteRuntimeInfo(String captureTaskId) {
        return Collections.emptyMap();
    }

    /**
     * 本地采集运行态摘要。约定键：{@code queueSize}、{@code queueCapacity}、{@code checkpoint}。
     */
    default Map<String, Object> getLocalRuntimeInfo(String captureTaskId) {
        return Collections.emptyMap();
    }

    /**
     * 本地采集已连接客户端列表。
     */
    default List<Map<String, Object>> listLocalClients(String captureTaskId) {
        return Collections.emptyList();
    }
}

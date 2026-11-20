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
     * 运维兜底：对已运行远端上报最小位点。主路径由 {@link #ensureRemote} 新建时上报。
     */
    default void minCheckpoint() {
    }

    /**
     * 仅启动本地采集；远端采集由同步任务 {@link #ensureRemote} 按需拉起。
     */
    default void start(String id) {
    }

    /**
     * 仅停止本地采集；远端采集由同步任务 {@link #releaseRemote} 按引用回收。
     */
    default void stop(String id) {
    }

    /**
     * 本机确保远端 Runtime：未运行则 start；总是 bindSchema；新建时上报本机 min。
     */
    default void ensureRemote(String captureRemoteId, String mappingId, String schema) {
    }

    /**
     * 本机释放远端订阅：unbindSchema；本机无引用则 stop Runtime。
     */
    default void releaseRemote(String captureRemoteId, String mappingId) {
    }

    /**
     * 本机该远端 Runtime 是否在跑（列表只读）。
     */
    default boolean isRemoteRunning(String captureRemoteId) {
        return false;
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

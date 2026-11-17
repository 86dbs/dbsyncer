/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

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

    void init();

    boolean isAvailable();

    void start(String id);

    void stop(String id);

    void bindSchema(String captureTaskId, String mappingId, String schema);

    void unbindSchema(String captureTaskId, String mappingId);

    /**
     * 远端采集运行态摘要。约定键：{@code queueSize}。
     */
    Map<String, Object> getRemoteRuntimeInfo(String captureTaskId);

    /**
     * 本地采集运行态摘要。约定键：{@code queueSize}、{@code queueCapacity}、{@code checkpoint}。
     */
    Map<String, Object> getLocalRuntimeInfo(String captureTaskId);

    /**
     * 本地采集已连接客户端列表。
     */
    List<Map<String, Object>> listLocalClients(String captureTaskId);
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * 采集任务启停扩展点
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

    default void start(String id) {
    }

    default void stop(String id) {
    }

    default void bindSchema(String captureTaskId, String mappingId, String schema) {
    }

    default void unbindSchema(String captureTaskId, String mappingId) {
    }
}

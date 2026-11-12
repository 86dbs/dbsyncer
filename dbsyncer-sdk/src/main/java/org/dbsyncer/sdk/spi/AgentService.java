/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * Agent 运行时扩展点。配置读写在开源实现；启动与停止由专业版实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
public interface AgentService {

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
     * 启动。
     *
     * @param id 主键
     */
    default void start(String id) {
        throw new UnsupportedOperationException("Agent 启停仅专业版可用");
    }

    /**
     * 停止。
     *
     * @param id 主键
     */
    default void stop(String id) {
        throw new UnsupportedOperationException("Agent 启停仅专业版可用");
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.cache;

/**
 * 缓存 key 常量（多业务共用 {@link CacheService} 时用前缀隔离）。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class CacheConstant {

    private CacheConstant() {
    }

    /**
     * 系统配置（{@code SystemConfig}）
     */
    public static final String SYSTEM_CONFIG = "parser:system:config";

    /**
     * 系统配置加载锁（防缓存击穿）
     */
    public static final String SYSTEM_CONFIG_LOCK = "parser:system:config:lock";

    /**
     * OpenAPI 防重放 nonce 前缀，完整 key = 前缀 + nonce
     */
    public static final String OPENAPI_NONCE_PREFIX = "openapi:nonce:";
}

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

    /**
     * OpenAPI 防重放 nonce 前缀，完整 key = 前缀 + nonce
     */
    public static final String OPENAPI_NONCE_PREFIX = "openapi:nonce:";
}

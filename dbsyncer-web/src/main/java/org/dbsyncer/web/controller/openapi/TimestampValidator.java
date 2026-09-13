/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import org.dbsyncer.common.cache.CacheConstant;
import org.dbsyncer.common.cache.CacheService;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 时间窗口与 Nonce 校验，防止 OpenAPI 重放攻击。
 *
 * @author 穿云
 * @version 2.0.0
 */
@Component
public class TimestampValidator {

    private static final Logger logger = LoggerFactory.getLogger(TimestampValidator.class);

    /**
     * 默认时间窗口：±5 分钟（毫秒）
     */
    public static final long DEFAULT_TIME_WINDOW = 5 * 60 * 1000L;

    private final CacheService cacheService;

    public TimestampValidator(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    /**
     * 验证时间戳是否在有效时间窗口内。
     *
     * @param timestamp  请求时间戳
     * @param timeWindow 时间窗口（毫秒）
     * @return 是否有效
     */
    public boolean validateTimestamp(long timestamp, long timeWindow) {
        long now = System.currentTimeMillis();
        long diff = Math.abs(now - timestamp);
        if (diff > timeWindow) {
            logger.warn("时间戳验证失败，当前时间: {}, 请求时间: {}, 差值: {}ms, 允许窗口: {}ms",
                    now, timestamp, diff, timeWindow);
            return false;
        }
        return true;
    }

    /**
     * 验证 Nonce 是否重复。
     *
     * @param nonce     随机数
     * @param timestamp 请求时间戳
     * @return true 有效；false 重复或无效
     */
    public boolean validateNonce(String nonce, long timestamp) {
        if (StringUtil.isBlank(nonce)) {
            logger.warn("Nonce为空");
            return false;
        }
        String key = CacheConstant.OPENAPI_NONCE_PREFIX + nonce;
        if (cacheService.containsKey(key)) {
            logger.warn("检测到重复的Nonce: {}", nonce);
            return false;
        }
        // Nonce 存活时间与时间窗口一致，过期后允许复用
        cacheService.put(key, timestamp, DEFAULT_TIME_WINDOW);
        return true;
    }

    /**
     * 验证时间戳和 Nonce。
     */
    public boolean validate(long timestamp, String nonce, long timeWindow) {
        if (!validateTimestamp(timestamp, timeWindow)) {
            return false;
        }
        return validateNonce(nonce, timestamp);
    }

    /**
     * 验证时间戳和 Nonce（默认时间窗口）。
     */
    public boolean validate(long timestamp, String nonce) {
        return validate(timestamp, nonce, DEFAULT_TIME_WINDOW);
    }

}

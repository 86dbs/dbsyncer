/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间窗口验证工具
 * 用于防止重放攻击，验证请求时间戳的有效性
 * 
 * @author 穿云
 * @version 2.0.0
 */
public class TimestampValidator {

    private static final Logger logger = LoggerFactory.getLogger(TimestampValidator.class);
    
    // 默认时间窗口：±5分钟（毫秒）
    private static final long DEFAULT_TIME_WINDOW = 5 * 60 * 1000L;
    
    // Nonce缓存，用于防止重放攻击（key: nonce, value: timestamp）
    // 缓存时间：10分钟，清理间隔：1分钟
    private static final ExpiringCache<String, Long> nonceCache = 
            new ExpiringCache<>(10 * 60 * 1000L, 60 * 1000L);
    
    // 静态初始化块，确保缓存清理任务启动
    static {
        nonceCache.init();
        // 注册JVM关闭钩子，确保清理任务正确关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            nonceCache.destroy();
        }));
    }

    /**
     * 初始化缓存清理任务（如果需要手动初始化，可以调用此方法）
     */
    public static void init() {
        nonceCache.init();
    }

    /**
     * 验证时间戳是否在有效时间窗口内
     * 
     * @param timestamp 请求时间戳
     * @param timeWindow 时间窗口（毫秒），默认±5分钟
     * @return 是否有效
     */
    public static boolean validateTimestamp(long timestamp, long timeWindow) {
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
     * 验证时间戳（使用默认时间窗口±5分钟）
     * 
     * @param timestamp 请求时间戳
     * @return 是否有效
     */
    public static boolean validateTimestamp(long timestamp) {
        return validateTimestamp(timestamp, DEFAULT_TIME_WINDOW);
    }

    /**
     * 验证Nonce是否重复（防止重放攻击）
     * 
     * @param nonce 随机数
     * @param timestamp 请求时间戳
     * @return 是否有效（true-有效，false-重复或无效）
     */
    public static boolean validateNonce(String nonce, long timestamp) {
        if (StringUtil.isBlank(nonce)) {
            logger.warn("Nonce为空");
            return false;
        }
        
        // 检查nonce是否已存在
        if (nonceCache.containsKey(nonce)) {
            logger.warn("检测到重复的Nonce: {}", nonce);
            return false;
        }
        
        // 将nonce加入缓存
        nonceCache.put(nonce, timestamp);
        
        return true;
    }

    /**
     * 验证时间戳和Nonce
     * 
     * @param timestamp 请求时间戳
     * @param nonce 随机数
     * @param timeWindow 时间窗口（毫秒）
     * @return 是否有效
     */
    public static boolean validate(long timestamp, String nonce, long timeWindow) {
        // 先验证时间戳
        if (!validateTimestamp(timestamp, timeWindow)) {
            return false;
        }
        
        // 再验证nonce
        return validateNonce(nonce, timestamp);
    }

    /**
     * 验证时间戳和Nonce（使用默认时间窗口）
     * 
     * @param timestamp 请求时间戳
     * @param nonce 随机数
     * @return 是否有效
     */
    public static boolean validate(long timestamp, String nonce) {
        return validate(timestamp, nonce, DEFAULT_TIME_WINDOW);
    }

    /**
     * 获取默认时间窗口
     * 
     * @return 时间窗口（毫秒）
     */
    public static long getDefaultTimeWindow() {
        return DEFAULT_TIME_WINDOW;
    }
    
    /**
     * 获取Nonce缓存大小（用于监控）
     * 
     * @return 缓存大小
     */
    public static int getNonceCacheSize() {
        return nonceCache.size();
    }
}

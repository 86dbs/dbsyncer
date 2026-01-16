/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * AES密钥管理器
 * 支持长期有效的AES密钥，并支持动态变更
 * 使用ExpiringCache管理密钥的过期时间
 * 
 * @author 穿云
 * @version 2.0.0
 */
public class AESKeyManager {

    private static final Logger logger = LoggerFactory.getLogger(AESKeyManager.class);
    
    // 密钥缓存（key: appId, value: AES密钥）
    // 默认过期时间：30天，清理间隔：1小时
    private static final ExpiringCache<String, String> keyCache = 
            new ExpiringCache<>(30L * 24 * 60 * 60 * 1000, 60 * 60 * 1000L);
    
    // 读写锁，保证线程安全
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    // AES密钥长度
    private static final int AES_KEY_SIZE = 256;

    /**
     * 初始化缓存清理任务
     */
    public static void init() {
        keyCache.init();
    }

    /**
     * 获取或生成AES密钥
     * 如果密钥不存在或已过期，则生成新密钥
     * 
     * @param appId 业务系统标识
     * @return AES密钥（Base64编码）
     */
    public static String getOrGenerateKey(String appId) {
        lock.readLock().lock();
        try {
            String key = keyCache.get(appId);
            if (key != null) {
                return key;
            }
        } finally {
            lock.readLock().unlock();
        }
        
        // 需要生成新密钥
        lock.writeLock().lock();
        try {
            // 双重检查
            String key = keyCache.get(appId);
            if (key != null) {
                return key;
            }
            
            // 生成新密钥
            String newKey = generateAESKey();
            long expireTime = 30L * 24 * 60 * 60 * 1000; // 默认30天
            keyCache.put(appId, newKey, expireTime);
            logger.info("为业务系统 {} 生成新的AES密钥，过期时间: {} 天", appId, expireTime / (24 * 60 * 60 * 1000));
            return newKey;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取AES密钥（不自动生成）
     * 
     * @param appId 业务系统标识
     * @return AES密钥，如果不存在或已过期返回null
     */
    public static String getKey(String appId) {
        lock.readLock().lock();
        try {
            return keyCache.get(appId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 更新AES密钥（强制更新）
     * 
     * @param appId 业务系统标识
     * @return 新的AES密钥
     */
    public static String updateKey(String appId) {
        lock.writeLock().lock();
        try {
            String newKey = generateAESKey();
            long expireTime = 30L * 24 * 60 * 60 * 1000; // 默认30天
            keyCache.put(appId, newKey, expireTime);
            logger.info("更新业务系统 {} 的AES密钥，过期时间: {} 天", appId, expireTime / (24 * 60 * 60 * 1000));
            return newKey;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 更新AES密钥（指定过期时间）
     * 
     * @param appId 业务系统标识
     * @param expireDays 过期天数
     * @return 新的AES密钥
     */
    public static String updateKey(String appId, int expireDays) {
        lock.writeLock().lock();
        try {
            String newKey = generateAESKey();
            long expireTime = expireDays * 24L * 60 * 60 * 1000;
            keyCache.put(appId, newKey, expireTime);
            logger.info("更新业务系统 {} 的AES密钥，过期时间: {} 天", appId, expireDays);
            return newKey;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 删除AES密钥
     * 
     * @param appId 业务系统标识
     */
    public static void removeKey(String appId) {
        lock.writeLock().lock();
        try {
            keyCache.remove(appId);
            logger.info("删除业务系统 {} 的AES密钥", appId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 检查密钥是否存在且有效
     * 
     * @param appId 业务系统标识
     * @return 是否有效
     */
    public static boolean isKeyValid(String appId) {
        lock.readLock().lock();
        try {
            return keyCache.containsKey(appId);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取密钥缓存大小（用于监控）
     * 
     * @return 缓存大小
     */
    public static int getKeyCacheSize() {
        return keyCache.size();
    }

    /**
     * 生成AES密钥
     */
    private static String generateAESKey() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            SecureRandom secureRandom = new SecureRandom();
            keyGenerator.init(AES_KEY_SIZE, secureRandom);
            SecretKey secretKey = keyGenerator.generateKey();
            return Base64.getEncoder().encodeToString(secretKey.getEncoded());
        } catch (Exception e) {
            logger.error("生成AES密钥失败", e);
            throw new RuntimeException("生成AES密钥失败", e);
        }
    }
}
/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 过期缓存工具类
 * 支持自定义过期时间，自动清理过期数据
 * 
 * @author 穿云
 * @version 1.0.0
 */
public class ExpiringCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ExpiringCache.class);
    
    // 缓存存储
    private final Map<K, CacheEntry<V>> cache = new ConcurrentHashMap<>();
    
    // 读写锁，保证线程安全
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    // 定时清理任务
    private ScheduledExecutorService cleanupExecutor;
    
    // 默认过期时间（毫秒）
    private final long defaultExpireTime;
    
    // 清理间隔（毫秒）
    private final long cleanupInterval;
    
    /**
     * 构造函数
     * 
     * @param defaultExpireTime 默认过期时间（毫秒）
     * @param cleanupInterval 清理间隔（毫秒）
     */
    public ExpiringCache(long defaultExpireTime, long cleanupInterval) {
        this.defaultExpireTime = defaultExpireTime;
        this.cleanupInterval = cleanupInterval;
    }
    
    /**
     * 初始化定时清理任务
     * 注意：需要手动调用此方法，或由Spring容器管理时自动调用
     */
    public void init() {
        cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ExpiringCache-Cleanup");
            t.setDaemon(true);
            return t;
        });
        
        cleanupExecutor.scheduleWithFixedDelay(
                this::cleanupExpiredEntries,
                cleanupInterval,
                cleanupInterval,
                TimeUnit.MILLISECONDS
        );
        
        logger.info("ExpiringCache定时清理任务已启动，清理间隔: {}ms", cleanupInterval);
    }
    
    /**
     * 销毁定时清理任务
     * 注意：需要手动调用此方法，或由Spring容器管理时自动调用
     */
    public void destroy() {
        if (cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
            cleanupExecutor.shutdown();
            try {
                if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("ExpiringCache定时清理任务已停止");
        }
    }
    
    /**
     * 放入缓存（使用默认过期时间）
     * 
     * @param key 键
     * @param value 值
     */
    public void put(K key, V value) {
        put(key, value, defaultExpireTime);
    }
    
    /**
     * 放入缓存（指定过期时间）
     * 
     * @param key 键
     * @param value 值
     * @param expireTime 过期时间（毫秒）
     */
    public void put(K key, V value, long expireTime) {
        lock.writeLock().lock();
        try {
            long expireAt = System.currentTimeMillis() + expireTime;
            cache.put(key, new CacheEntry<>(value, expireAt));
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 获取缓存值
     * 
     * @param key 键
     * @return 值，如果不存在或已过期返回null
     */
    public V get(K key) {
        lock.readLock().lock();
        try {
            CacheEntry<V> entry = cache.get(key);
            if (entry == null) {
                return null;
            }
            
            if (entry.isExpired()) {
                // 已过期，移除
                cache.remove(key);
                return null;
            }
            
            return entry.getValue();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 移除缓存
     * 
     * @param key 键
     * @return 被移除的值，如果不存在返回null
     */
    public V remove(K key) {
        lock.writeLock().lock();
        try {
            CacheEntry<V> entry = cache.remove(key);
            return entry != null ? entry.getValue() : null;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 检查键是否存在且未过期
     * 
     * @param key 键
     * @return 是否存在且有效
     */
    public boolean containsKey(K key) {
        lock.readLock().lock();
        try {
            CacheEntry<V> entry = cache.get(key);
            if (entry == null) {
                return false;
            }
            
            if (entry.isExpired()) {
                cache.remove(key);
                return false;
            }
            
            return true;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 清空所有缓存
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 获取缓存大小
     * 
     * @return 缓存大小
     */
    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 清理过期的条目
     */
    private void cleanupExpiredEntries() {
        lock.writeLock().lock();
        try {
            long now = System.currentTimeMillis();
            int removedCount = 0;
            
            for (K key : cache.keySet()) {
                CacheEntry<V> entry = cache.get(key);
                if (entry != null && entry.isExpired(now)) {
                    cache.remove(key);
                    removedCount++;
                }
            }
            
            if (removedCount > 0) {
                logger.debug("清理过期缓存条目: {} 个", removedCount);
            }
        } catch (Exception e) {
            logger.error("清理过期缓存条目失败", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 缓存条目
     */
    private static class CacheEntry<V> {
        private final V value;
        private final long expireAt;
        
        public CacheEntry(V value, long expireAt) {
            this.value = value;
            this.expireAt = expireAt;
        }
        
        public V getValue() {
            return value;
        }
        
        public long getExpireAt() {
            return expireAt;
        }
        
        public boolean isExpired() {
            return isExpired(System.currentTimeMillis());
        }
        
        public boolean isExpired(long now) {
            return now > expireAt;
        }
    }
}

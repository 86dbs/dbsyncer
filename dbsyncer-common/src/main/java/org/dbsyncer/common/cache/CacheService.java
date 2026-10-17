/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.cache;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 本地缓存服务（语义对齐 Redis TTL），以 Spring Bean 方式注入使用。
 * <p>
 * 多业务共用同一实例时，请用 {@link CacheConstant} 前缀隔离 key。
 * <p>
 * 锁与缓存数据分表存储，lockKey 可与缓存 key 相同，互不覆盖。
 *
 * @author 穿云
 * @version 1.0.0
 */
public interface CacheService {

    /**
     * 键不存在（对齐 Redis TTL）
     */
    long TTL_NOT_EXISTS = -2L;

    /**
     * 键存在但永不过期（对齐 Redis TTL）
     */
    long TTL_NO_EXPIRE = -1L;

    /**
     * 写入（默认永不过期）。
     */
    void put(String key, Object value);

    /**
     * 写入并设置过期毫秒；{@code expireTimeMs <= 0} 表示永不过期。
     */
    void put(String key, Object value, long expireTimeMs);

    /**
     * 写入并设置过期时间。
     */
    void put(String key, Object value, long expireTime, TimeUnit unit);

    /**
     * 获取值；不存在或已过期返回 null。
     */
    Object get(String key);

    /**
     * 获取并转为指定类型；类型不匹配返回 null。
     */
    <T> T get(String key, Class<T> type);

    /**
     * 仅刷新 TTL；键不存在或已过期返回 false。
     */
    boolean expire(String key, long expireTimeMs);

    /**
     * 仅刷新 TTL。
     */
    boolean expire(String key, long expireTime, TimeUnit unit);

    /**
     * 剩余存活时间（毫秒）：不存在 {@link #TTL_NOT_EXISTS}；永不过期 {@link #TTL_NO_EXPIRE}；否则 {@code >= 0}。
     */
    long ttl(String key);

    /**
     * 移除过期时间，变为永不过期。
     */
    boolean persist(String key);

    /**
     * 删除键，返回旧值。
     */
    Object remove(String key);

    /**
     * 键是否存在且未过期。
     */
    boolean containsKey(String key);

    /**
     * 清空全部。
     */
    void clear();

    /**
     * 当前条目数（可能含尚未扫掉的过期项）。
     */
    int size();

    /**
     * 按 lockKey 阻塞加锁（可重入）。
     */
    void lock(String lockKey);

    /**
     * 尝试加锁，立即返回。
     *
     * @return 是否获取成功
     */
    boolean tryLock(String lockKey);

    /**
     * 在超时时间内尝试加锁。
     *
     * @return 是否获取成功；等待被中断时恢复中断标记并返回 false
     */
    boolean tryLock(String lockKey, long waitTime, TimeUnit unit);

    /**
     * 释放当前线程持有的锁；未持有时忽略。
     */
    void unlock(String lockKey);

    /**
     * 加锁执行，结束后自动解锁。
     */
    default void executeWithLock(String lockKey, Runnable action) {
        lock(lockKey);
        try {
            action.run();
        } finally {
            unlock(lockKey);
        }
    }

    /**
     * 加锁执行并返回结果，结束后自动解锁。
     */
    default <T> T executeWithLock(String lockKey, Supplier<T> action) {
        lock(lockKey);
        try {
            return action.get();
        } finally {
            unlock(lockKey);
        }
    }
}

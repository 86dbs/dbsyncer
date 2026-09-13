/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于内存的 {@link CacheService} 实现：惰性删除 + 定时扫表。
 *
 * @author 穿云
 * @version 1.0.0
 */
@Component
public class CacheServiceImpl implements CacheService, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(CacheServiceImpl.class);

    private static final long CLEANUP_INTERVAL_MS = 30_000L;

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile ScheduledExecutorService cleanupExecutor;

    public CacheServiceImpl() {
        startCleanup();
    }

    @Override
    public void put(String key, Object value) {
        put(key, value, 0L);
    }

    @Override
    public void put(String key, Object value, long expireTimeMs) {
        Objects.requireNonNull(key, "key");
        cache.put(key, new CacheEntry(value, toExpireAt(expireTimeMs)));
    }

    @Override
    public void put(String key, Object value, long expireTime, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        put(key, value, unit.toMillis(expireTime));
    }

    @Override
    public Object get(String key) {
        Objects.requireNonNull(key, "key");
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            cache.remove(key, entry);
            return null;
        }
        return entry.value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> type) {
        Objects.requireNonNull(type, "type");
        Object value = get(key);
        if (value == null || !type.isInstance(value)) {
            return null;
        }
        return (T) value;
    }

    @Override
    public boolean expire(String key, long expireTimeMs) {
        Objects.requireNonNull(key, "key");
        CacheEntry current = cache.get(key);
        if (current == null || current.isExpired()) {
            if (current != null) {
                cache.remove(key, current);
            }
            return false;
        }
        return cache.replace(key, current, new CacheEntry(current.value, toExpireAt(expireTimeMs)));
    }

    @Override
    public boolean expire(String key, long expireTime, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        return expire(key, unit.toMillis(expireTime));
    }

    @Override
    public long ttl(String key) {
        Objects.requireNonNull(key, "key");
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return TTL_NOT_EXISTS;
        }
        if (entry.isExpired()) {
            cache.remove(key, entry);
            return TTL_NOT_EXISTS;
        }
        if (entry.expireAt == Long.MAX_VALUE) {
            return TTL_NO_EXPIRE;
        }
        return Math.max(0L, entry.expireAt - System.currentTimeMillis());
    }

    @Override
    public boolean persist(String key) {
        return expire(key, 0L);
    }

    @Override
    public Object remove(String key) {
        Objects.requireNonNull(key, "key");
        CacheEntry entry = cache.remove(key);
        return entry != null ? entry.value : null;
    }

    @Override
    public boolean containsKey(String key) {
        Objects.requireNonNull(key, "key");
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return false;
        }
        if (entry.isExpired()) {
            cache.remove(key, entry);
            return false;
        }
        return true;
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void destroy() {
        stopCleanup();
        cache.clear();
    }

    private void startCleanup() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "CacheService-Cleanup");
            t.setDaemon(true);
            return t;
        });
        cleanupExecutor.scheduleWithFixedDelay(this::cleanupExpiredEntries,
                CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
        logger.debug("CacheService 已启动, cleanupInterval={}ms", CLEANUP_INTERVAL_MS);
    }

    private void stopCleanup() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        ScheduledExecutorService executor = cleanupExecutor;
        cleanupExecutor = null;
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.debug("CacheService 已停止");
    }

    private void cleanupExpiredEntries() {
        try {
            long now = System.currentTimeMillis();
            int removed = 0;
            Iterator<Map.Entry<String, CacheEntry>> it = cache.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, CacheEntry> e = it.next();
                CacheEntry entry = e.getValue();
                if (entry != null && entry.isExpired(now)) {
                    it.remove();
                    removed++;
                }
            }
            if (removed > 0) {
                logger.debug("CacheService 清理过期条目: {}", removed);
            }
        } catch (Exception e) {
            logger.error("CacheService 清理失败", e);
        }
    }

    private static long toExpireAt(long expireTimeMs) {
        if (expireTimeMs <= 0L) {
            return Long.MAX_VALUE;
        }
        long now = System.currentTimeMillis();
        long expireAt = now + expireTimeMs;
        return expireAt < now ? Long.MAX_VALUE : expireAt;
    }

    private static final class CacheEntry {
        private final Object value;
        private final long expireAt;

        private CacheEntry(Object value, long expireAt) {
            this.value = value;
            this.expireAt = expireAt;
        }

        private boolean isExpired() {
            return isExpired(System.currentTimeMillis());
        }

        private boolean isExpired(long now) {
            return expireAt != Long.MAX_VALUE && now > expireAt;
        }
    }
}

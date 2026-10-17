/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.cache;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link CacheServiceImpl} 单元测试。
 *
 * @author 穿云
 * @version 1.0.0
 */
public class CacheServiceTest {

    private CacheServiceImpl cacheService;

    @Before
    public void setUp() {
        cacheService = new CacheServiceImpl();
    }

    @After
    public void tearDown() {
        if (cacheService != null) {
            cacheService.destroy();
        }
    }

    @Test
    public void testPutAndGet() {
        cacheService.put("k1", "v1");
        Assert.assertEquals("v1", cacheService.get("k1"));
        Assert.assertEquals("v1", cacheService.get("k1", String.class));
    }

    @Test
    public void testCustomExpire() throws InterruptedException {
        cacheService.put("k1", "v1", 100L);
        Assert.assertEquals("v1", cacheService.get("k1"));
        Thread.sleep(150L);
        Assert.assertNull(cacheService.get("k1"));
        Assert.assertEquals(CacheService.TTL_NOT_EXISTS, cacheService.ttl("k1"));
    }

    @Test
    public void testExpireAndTtl() throws InterruptedException {
        cacheService.put("k1", "v1", 500L);
        long ttl = cacheService.ttl("k1");
        Assert.assertTrue(ttl > 0 && ttl <= 500L);

        Assert.assertTrue(cacheService.expire("k1", 200L));
        ttl = cacheService.ttl("k1");
        Assert.assertTrue(ttl > 0 && ttl <= 200L);

        Thread.sleep(250L);
        Assert.assertFalse(cacheService.expire("k1", 1000L));
        Assert.assertEquals(CacheService.TTL_NOT_EXISTS, cacheService.ttl("missing"));
    }

    @Test
    public void testPersist() {
        cacheService.put("k1", "v1", 1000L);
        Assert.assertTrue(cacheService.persist("k1"));
        Assert.assertEquals(CacheService.TTL_NO_EXPIRE, cacheService.ttl("k1"));
        Assert.assertEquals("v1", cacheService.get("k1"));
    }

    @Test
    public void testPutWithTimeUnit() throws InterruptedException {
        cacheService.put("k1", "v1", 100L, TimeUnit.MILLISECONDS);
        Assert.assertTrue(cacheService.containsKey("k1"));
        Thread.sleep(150L);
        Assert.assertFalse(cacheService.containsKey("k1"));
    }

    @Test
    public void testRemoveClearSize() {
        cacheService.put("k1", "v1");
        cacheService.put("k2", "v2");
        Assert.assertEquals(2, cacheService.size());
        Assert.assertEquals("v1", cacheService.remove("k1"));
        Assert.assertEquals(1, cacheService.size());
        cacheService.clear();
        Assert.assertEquals(0, cacheService.size());
    }

    @Test
    public void testExecuteWithLock() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        Runnable task = () -> {
            try {
                start.await();
                cacheService.executeWithLock("lock1", () -> {
                    int current = counter.get();
                    try {
                        Thread.sleep(50L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    counter.set(current + 1);
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        };

        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);
        t1.start();
        t2.start();
        start.countDown();
        Assert.assertTrue(done.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(2, counter.get());
    }

    @Test
    public void testTryLock() throws InterruptedException {
        Assert.assertTrue(cacheService.tryLock("lock2"));
        AtomicInteger otherGotLock = new AtomicInteger(-1);
        Thread other = new Thread(() -> otherGotLock.set(cacheService.tryLock("lock2") ? 1 : 0));
        other.start();
        other.join(1000L);
        Assert.assertEquals(0, otherGotLock.get());
        cacheService.unlock("lock2");
        Assert.assertTrue(cacheService.tryLock("lock2"));
        cacheService.unlock("lock2");
    }

    @Test
    public void testLockIndependentFromCacheValue() {
        cacheService.put("same-key", "value");
        cacheService.lock("same-key");
        Assert.assertEquals("value", cacheService.get("same-key"));
        cacheService.unlock("same-key");
    }
}

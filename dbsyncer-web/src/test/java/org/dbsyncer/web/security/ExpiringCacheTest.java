/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 过期缓存工具类测试
 * 
 * @author 穿云
 * @version 1.0.0
 */
public class ExpiringCacheTest {

    private static final Logger logger = LoggerFactory.getLogger(ExpiringCacheTest.class);

    private ExpiringCache<String, String> cache;

    @Before
    public void setUp() {
        // 创建缓存：默认过期时间1秒，清理间隔500毫秒
        cache = new ExpiringCache<>(1000L, 500L);
        cache.init();
    }

    @After
    public void tearDown() {
        if (cache != null) {
            cache.destroy();
        }
    }

    @Test
    public void testPutAndGet() throws InterruptedException {
        logger.info("=== 测试放入和获取缓存 ===");

        // 放入缓存
        cache.put("key1", "value1");
        Assert.assertEquals("应能获取到值", "value1", cache.get("key1"));

        // 使用默认过期时间
        cache.put("key2", "value2");
        Assert.assertEquals("应能获取到值", "value2", cache.get("key2"));

        logger.info("放入和获取缓存测试通过");
    }

    @Test
    public void testExpiration() throws InterruptedException {
        logger.info("=== 测试缓存过期 ===");

        // 放入缓存，过期时间100毫秒
        cache.put("key1", "value1", 100L);
        Assert.assertEquals("应能获取到值", "value1", cache.get("key1"));

        // 等待过期
        Thread.sleep(150);

        // 应已过期
        Assert.assertNull("过期后应返回null", cache.get("key1"));
        logger.info("缓存过期测试通过");
    }

    @Test
    public void testRemove() {
        logger.info("=== 测试移除缓存 ===");

        cache.put("key1", "value1");
        Assert.assertEquals("应能获取到值", "value1", cache.get("key1"));

        // 移除
        String removed = cache.remove("key1");
        Assert.assertEquals("移除应返回原值", "value1", removed);
        Assert.assertNull("移除后应返回null", cache.get("key1"));

        logger.info("移除缓存测试通过");
    }

    @Test
    public void testContainsKey() throws InterruptedException {
        logger.info("=== 测试检查键是否存在 ===");

        // 不存在
        Assert.assertFalse("不存在的键应返回false", cache.containsKey("key1"));

        // 存在
        cache.put("key1", "value1");
        Assert.assertTrue("存在的键应返回true", cache.containsKey("key1"));

        // 过期后
        cache.put("key2", "value2", 100L);
        Thread.sleep(150);
        Assert.assertFalse("过期后应返回false", cache.containsKey("key2"));

        logger.info("检查键是否存在测试通过");
    }

    @Test
    public void testClear() {
        logger.info("=== 测试清空缓存 ===");

        cache.put("key1", "value1");
        cache.put("key2", "value2");
        Assert.assertEquals("缓存大小应为2", 2, cache.size());

        cache.clear();
        Assert.assertEquals("清空后大小应为0", 0, cache.size());
        Assert.assertNull("清空后应返回null", cache.get("key1"));

        logger.info("清空缓存测试通过");
    }

    @Test
    public void testSize() {
        logger.info("=== 测试缓存大小 ===");

        Assert.assertEquals("初始大小应为0", 0, cache.size());

        cache.put("key1", "value1");
        Assert.assertEquals("大小应为1", 1, cache.size());

        cache.put("key2", "value2");
        Assert.assertEquals("大小应为2", 2, cache.size());

        cache.remove("key1");
        Assert.assertEquals("移除后大小应为1", 1, cache.size());

        logger.info("缓存大小测试通过");
    }

    @Test
    public void testAutoCleanup() throws InterruptedException {
        logger.info("=== 测试自动清理过期条目 ===");

        // 放入多个缓存项，过期时间不同
        cache.put("key1", "value1", 100L);
        cache.put("key2", "value2", 200L);
        cache.put("key3", "value3", 300L);

        Assert.assertEquals("初始大小应为3", 3, cache.size());

        // 等待第一个过期
        Thread.sleep(150);
        // 等待清理任务执行（清理间隔500ms，但可能还没执行，手动触发一次）
        Thread.sleep(600);

        // 第一个应该被清理了
        Assert.assertNull("key1应已被清理", cache.get("key1"));
        Assert.assertNotNull("key2应还存在", cache.get("key2"));
        Assert.assertNotNull("key3应还存在", cache.get("key3"));

        logger.info("自动清理过期条目测试通过");
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        logger.info("=== 测试并发访问 ===");

        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];

        // 并发写入
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(()-> {
                cache.put("key" + index, "value" + index);
            });
            threads[i].start();
        }

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }

        // 验证所有值都存在
        Assert.assertEquals("缓存大小应为10", threadCount, cache.size());
        for (int i = 0; i < threadCount; i++) {
            Assert.assertEquals("应能获取到值", "value" + i, cache.get("key" + i));
        }

        logger.info("并发访问测试通过");
    }
}

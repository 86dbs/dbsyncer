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
 * AES密钥管理器测试
 * 
 * @author 穿云
 * @version 2.0.0
 */
public class AESKeyManagerTest {

    private static final Logger logger = LoggerFactory.getLogger(AESKeyManagerTest.class);
    
    private static final String TEST_APP_ID = "test-app-001";

    @Before
    public void setUp() {
        // 初始化缓存清理任务
        AESKeyManager.init();
    }

    @After
    public void tearDown() {
        // 清理测试数据
        AESKeyManager.removeKey(TEST_APP_ID);
    }

    @Test
    public void testGetOrGenerateKey() {
        logger.info("=== 测试获取或生成密钥 ===");
        
        // 第一次获取，应生成新密钥
        String key1 = AESKeyManager.getOrGenerateKey(TEST_APP_ID);
        Assert.assertNotNull("密钥不应为空", key1);
        logger.info("生成的密钥: {}", key1);
        
        // 第二次获取，应返回相同密钥
        String key2 = AESKeyManager.getOrGenerateKey(TEST_APP_ID);
        Assert.assertEquals("应返回相同密钥", key1, key2);
        logger.info("获取相同密钥测试通过");
    }

    @Test
    public void testGetKey() {
        logger.info("=== 测试获取密钥（不自动生成） ===");
        
        // 未生成密钥时，应返回null
        String key = AESKeyManager.getKey(TEST_APP_ID);
        Assert.assertNull("未生成密钥时应返回null", key);
        
        // 生成密钥后，应能获取
        AESKeyManager.getOrGenerateKey(TEST_APP_ID);
        key = AESKeyManager.getKey(TEST_APP_ID);
        Assert.assertNotNull("生成密钥后应能获取", key);
        logger.info("获取密钥测试通过");
    }

    @Test
    public void testUpdateKey() {
        logger.info("=== 测试更新密钥 ===");
        
        // 生成初始密钥
        String key1 = AESKeyManager.getOrGenerateKey(TEST_APP_ID);
        
        // 更新密钥
        String key2 = AESKeyManager.updateKey(TEST_APP_ID);
        Assert.assertNotNull("新密钥不应为空", key2);
        Assert.assertNotEquals("新密钥应不同", key1, key2);
        
        // 验证新密钥已生效
        String key3 = AESKeyManager.getKey(TEST_APP_ID);
        Assert.assertEquals("应返回新密钥", key2, key3);
        logger.info("更新密钥测试通过");
    }

    @Test
    public void testUpdateKeyWithExpireDays() {
        logger.info("=== 测试更新密钥（指定过期天数） ===");
        
        String key = AESKeyManager.updateKey(TEST_APP_ID, 60);
        Assert.assertNotNull("密钥不应为空", key);
        Assert.assertTrue("密钥应有效", AESKeyManager.isKeyValid(TEST_APP_ID));
        logger.info("指定过期天数更新密钥测试通过");
    }

    @Test
    public void testRemoveKey() {
        logger.info("=== 测试删除密钥 ===");
        
        // 生成密钥
        AESKeyManager.getOrGenerateKey(TEST_APP_ID);
        Assert.assertTrue("密钥应存在", AESKeyManager.isKeyValid(TEST_APP_ID));
        
        // 删除密钥
        AESKeyManager.removeKey(TEST_APP_ID);
        Assert.assertFalse("密钥应不存在", AESKeyManager.isKeyValid(TEST_APP_ID));
        Assert.assertNull("获取密钥应返回null", AESKeyManager.getKey(TEST_APP_ID));
        logger.info("删除密钥测试通过");
    }

    @Test
    public void testIsKeyValid() {
        logger.info("=== 测试密钥有效性 ===");
        
        // 未生成密钥时，应无效
        Assert.assertFalse("未生成密钥时应无效", AESKeyManager.isKeyValid(TEST_APP_ID));
        
        // 生成密钥后，应有效
        AESKeyManager.getOrGenerateKey(TEST_APP_ID);
        Assert.assertTrue("生成密钥后应有效", AESKeyManager.isKeyValid(TEST_APP_ID));
        
        // 删除密钥后，应无效
        AESKeyManager.removeKey(TEST_APP_ID);
        Assert.assertFalse("删除密钥后应无效", AESKeyManager.isKeyValid(TEST_APP_ID));
        logger.info("密钥有效性测试通过");
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        logger.info("=== 测试并发访问 ===");
        
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        String[] keys = new String[threadCount];
        
        // 并发获取密钥
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                keys[index] = AESKeyManager.getOrGenerateKey(TEST_APP_ID);
            });
            threads[i].start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
        
        // 验证所有线程获取的密钥相同
        String firstKey = keys[0];
        for (int i = 1; i < threadCount; i++) {
            Assert.assertEquals("并发访问应返回相同密钥", firstKey, keys[i]);
        }
        logger.info("并发访问测试通过，所有线程获取的密钥相同");
    }
}

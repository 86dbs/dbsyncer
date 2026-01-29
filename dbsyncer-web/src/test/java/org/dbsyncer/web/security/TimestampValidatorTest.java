/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间窗口验证工具测试
 * 
 * @author 穿云
 * @version 2.0.0
 */
public class TimestampValidatorTest {

    private static final Logger logger = LoggerFactory.getLogger(TimestampValidatorTest.class);

    @Test
    public void testValidateTimestamp_Valid() {
        logger.info("=== 测试有效时间戳 ===");
        long now = System.currentTimeMillis();
        boolean isValid = TimestampValidator.validateTimestamp(now);
        Assert.assertTrue("当前时间戳应有效", isValid);
        logger.info("有效时间戳测试通过");
    }

    @Test
    public void testValidateTimestamp_Expired() {
        logger.info("=== 测试过期时间戳 ===");
        // 10分钟前的时间戳（超过默认5分钟窗口）
        long expiredTimestamp = System.currentTimeMillis() - (10 * 60 * 1000L);
        boolean isValid = TimestampValidator.validateTimestamp(expiredTimestamp);
        Assert.assertFalse("过期时间戳应无效", isValid);
        logger.info("过期时间戳测试通过");
    }

    @Test
    public void testValidateTimestamp_Future() {
        logger.info("=== 测试未来时间戳 ===");
        // 10分钟后的时间戳（超过默认5分钟窗口）
        long futureTimestamp = System.currentTimeMillis() + (10 * 60 * 1000L);
        boolean isValid = TimestampValidator.validateTimestamp(futureTimestamp);
        Assert.assertFalse("未来时间戳应无效", isValid);
        logger.info("未来时间戳测试通过");
    }

    @Test
    public void testValidateTimestamp_CustomWindow() {
        logger.info("=== 测试自定义时间窗口 ===");
        long now = System.currentTimeMillis();
        // 使用10分钟窗口
        boolean isValid = TimestampValidator.validateTimestamp(now, 10 * 60 * 1000L);
        Assert.assertTrue("当前时间戳应在10分钟窗口内有效", isValid);
        
        // 8分钟前的时间戳，应在10分钟窗口内有效
        long timestamp8minAgo = now - (8 * 60 * 1000L);
        isValid = TimestampValidator.validateTimestamp(timestamp8minAgo, 10 * 60 * 1000L);
        Assert.assertTrue("8分钟前的时间戳应在10分钟窗口内有效", isValid);
        
        logger.info("自定义时间窗口测试通过");
    }

    @Test
    public void testValidateNonce_Valid() {
        logger.info("=== 测试有效Nonce ===");
        String nonce = "test-nonce-001";
        long timestamp = System.currentTimeMillis();
        
        boolean isValid = TimestampValidator.validateNonce(nonce, timestamp);
        Assert.assertTrue("新Nonce应有效", isValid);
        logger.info("有效Nonce测试通过");
    }

    @Test
    public void testValidateNonce_Duplicate() {
        logger.info("=== 测试重复Nonce ===");
        String nonce = "test-nonce-002";
        long timestamp = System.currentTimeMillis();
        
        // 第一次使用，应有效
        boolean isValid1 = TimestampValidator.validateNonce(nonce, timestamp);
        Assert.assertTrue("第一次使用Nonce应有效", isValid1);
        
        // 第二次使用，应无效（重复）
        boolean isValid2 = TimestampValidator.validateNonce(nonce, timestamp);
        Assert.assertFalse("重复Nonce应无效", isValid2);
        logger.info("重复Nonce测试通过");
    }

    @Test
    public void testValidateNonce_Blank() {
        logger.info("=== 测试空Nonce ===");
        boolean isValid = TimestampValidator.validateNonce("", System.currentTimeMillis());
        Assert.assertFalse("空Nonce应无效", isValid);
        
        isValid = TimestampValidator.validateNonce(null, System.currentTimeMillis());
        Assert.assertFalse("null Nonce应无效", isValid);
        logger.info("空Nonce测试通过");
    }

    @Test
    public void testValidate_Combined() {
        logger.info("=== 测试组合验证（时间戳+Nonce） ===");
        long timestamp = System.currentTimeMillis();
        String nonce = "test-nonce-003";
        
        // 有效的时间戳和Nonce
        boolean isValid = TimestampValidator.validate(timestamp, nonce);
        Assert.assertTrue("有效的时间戳和Nonce应通过验证", isValid);
        
        // 过期的时间戳
        long expiredTimestamp = System.currentTimeMillis() - (10 * 60 * 1000L);
        isValid = TimestampValidator.validate(expiredTimestamp, "test-nonce-004");
        Assert.assertFalse("过期时间戳应验证失败", isValid);
        
        // 重复的Nonce
        isValid = TimestampValidator.validate(timestamp, nonce);
        Assert.assertFalse("重复Nonce应验证失败", isValid);
        
        logger.info("组合验证测试通过");
    }

    @Test
    public void testGetDefaultTimeWindow() {
        logger.info("=== 测试获取默认时间窗口 ===");
        long defaultWindow = TimestampValidator.getDefaultTimeWindow();
        Assert.assertEquals("默认时间窗口应为5分钟", 5 * 60 * 1000L, defaultWindow);
        logger.info("默认时间窗口: {}ms ({}分钟)", defaultWindow, defaultWindow / 60 / 1000);
    }
}

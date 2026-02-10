/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.dbsyncer.common.model.AesData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 加密工具类测试
 *
 * @author 穿云
 * @version 1.0.0
 */
public class CryptoUtilTest {

    private static final Logger logger = LoggerFactory.getLogger(CryptoUtilTest.class);

    private Map<String, Object> testData;

    @Before
    public void setUp() {
        // 生成RSA密钥对
        testData = new HashMap<>();
        testData.put("name", "测试数据");
        testData.put("value", 12345);
        testData.put("timestamp", System.currentTimeMillis());
        logger.info("测试数据: {}", testData);
    }

    @Test
    public void testGenerateAESKey() {
        logger.info("=== 测试生成AES密钥 ===");
        String aesKey = CryptoUtil.generateAESKeyPair();
        Assert.assertNotNull("AES密钥不应为空", aesKey);
        logger.info("生成的AES密钥: {}", aesKey);
    }

    @Test
    public void testLargeDataEncryption() {
        logger.info("=== 测试大数据加密 ===");
        // 生成较大的测试数据（约100KB）
        StringBuilder largeData = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeData.append("这是测试数据").append(i).append(",");
        }
        String largeDataStr = largeData.toString();

        String aesKey = CryptoUtil.generateAESKeyPair();

        long startTime = System.currentTimeMillis();
        AesData encryptedResult = CryptoUtil.encryptData(largeDataStr, aesKey);
        long encryptTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        String decryptedData = CryptoUtil.decryptData(encryptedResult.getEncryptedData(), encryptedResult.getIv(), aesKey);
        long decryptTime = System.currentTimeMillis() - startTime;

        Assert.assertEquals("大数据解密后应相同", largeDataStr, decryptedData);
        logger.info("大数据加密耗时 - 加密: {}ms, 解密: {}ms", encryptTime, decryptTime);
    }
}

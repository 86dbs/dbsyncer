/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.dbsyncer.common.model.RsaVersion;

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

    private RsaVersion rsaConfig;
    private Map<String, Object> testData;

    @Before
    public void setUp() {
        // 生成RSA密钥对
        rsaConfig = RSAUtil.createKeys(2048);
        testData = new HashMap<>();
        testData.put("name", "测试数据");
        testData.put("value", 12345);
        testData.put("timestamp", System.currentTimeMillis());
        logger.info("测试数据: {}", testData);
    }

    @Test
    public void testGenerateAESKey() {
        logger.info("=== 测试生成AES密钥 ===");
        Map<String, String> keyPair = CryptoUtil.generateAESKeyPair();
        Assert.assertNotNull("AES密钥不应为空", keyPair);
        Assert.assertNotNull("AES密钥值不应为空", keyPair.get("aesKey"));
        logger.info("生成的AES密钥: {}", keyPair.get("aesKey"));
    }

    @Test
    public void testEncryptAndDecryptAESKey() {
        logger.info("=== 测试RSA加密/解密AES密钥 ===");
        Map<String, String> aesKeyPair = CryptoUtil.generateAESKeyPair();
        String aesKey = aesKeyPair.get("aesKey");

        // RSA加密AES密钥
        String encryptedKey = CryptoUtil.encryptAESKey(aesKey, rsaConfig.getPublicKey());
        Assert.assertNotNull("加密后的密钥不应为空", encryptedKey);
        Assert.assertNotEquals("加密后的密钥应不同", aesKey, encryptedKey);

        // RSA解密AES密钥
        String decryptedKey = CryptoUtil.decryptAESKey(encryptedKey, rsaConfig.getPrivateKey());
        Assert.assertEquals("解密后的密钥应相同", aesKey, decryptedKey);
        logger.info("RSA加密/解密AES密钥测试通过");
    }

    @Test
    public void testEncryptAndDecryptData() {
        logger.info("=== 测试AES加密/解密数据 ===");
        Map<String, String> aesKeyPair = CryptoUtil.generateAESKeyPair();
        String aesKey = aesKeyPair.get("aesKey");

        // 将testData序列化为JSON字符串
        String jsonData = JsonUtil.objToJson(testData);

        // AES加密数据
        Map<String, String> encryptedResult = CryptoUtil.encryptData(jsonData, aesKey);
        Assert.assertNotNull("加密结果不应为空", encryptedResult);
        Assert.assertNotNull("加密数据不应为空", encryptedResult.get("encryptedData"));
        Assert.assertNotNull("IV不应为空", encryptedResult.get("iv"));

        // AES解密数据
        String decryptedData = CryptoUtil.decryptData(encryptedResult.get("encryptedData"), encryptedResult.get("iv"), aesKey);
        Assert.assertEquals("解密后的数据应相同", jsonData, decryptedData);

        // 验证解密后的数据可以反序列化为Map
        Map<String, Object> decryptedMap = JsonUtil.jsonToObj(decryptedData, Map.class);
        Assert.assertEquals("解密后的Map应相同", testData.get("name"), decryptedMap.get("name"));
        logger.info("AES加密/解密数据测试通过");
    }

    @Test
    public void testBuildAndParseEncryptedRequest_PublicNetwork() {
        logger.info("=== 测试构建和解析加密请求（公网场景） ===");
        boolean isPublicNetwork = true;

        // 构建加密请求（公网场景需要私钥签名）
        String encryptedRequest = CryptoUtil.buildEncryptedRequest(testData, rsaConfig.getPublicKey(), rsaConfig.getPrivateKey(), null, // 公网场景不需要HMAC密钥
                isPublicNetwork);
        Assert.assertNotNull("加密请求不应为空", encryptedRequest);
        logger.info("加密请求: {}", encryptedRequest);

        // 解析加密请求（公网场景使用公钥验证签名）
        Map<String, Object> decryptedData = CryptoUtil.parseEncryptedRequest(encryptedRequest, rsaConfig.getPrivateKey(), rsaConfig.getPublicKey(), null, // 内网场景才需要HMAC密钥
                isPublicNetwork, Map.class);
        Assert.assertNotNull("解密后的数据不应为空", decryptedData);
        logger.info("解密后的数据: {}", decryptedData);
    }

    @Test
    public void testBuildAndParseEncryptedRequest_PrivateNetwork() {
        logger.info("=== 测试构建和解析加密请求（内网场景） ===");
        boolean isPublicNetwork = false;
        String hmacSecret = "test-hmac-secret-key-12345"; // 内网场景的HMAC密钥

        // 构建加密请求（内网场景使用HMAC签名，不需要私钥）
        String encryptedRequest = CryptoUtil.buildEncryptedRequest(testData, rsaConfig.getPublicKey(), null, // 内网场景不需要私钥
                hmacSecret, isPublicNetwork);
        Assert.assertNotNull("加密请求不应为空", encryptedRequest);

        // 解析加密请求（内网场景使用HMAC密钥验证签名）
        Map<String, Object> decryptedData = CryptoUtil.parseEncryptedRequest(encryptedRequest, rsaConfig.getPrivateKey(), null, // 内网场景不需要公钥
                hmacSecret, isPublicNetwork, Map.class);
        Assert.assertNotNull("解密后的数据不应为空", decryptedData);
        logger.info("内网场景测试通过");
    }

    @Test
    public void testSignature_PublicNetwork() {
        logger.info("=== 测试签名（公网场景） ===");
        String data = "test-signature-data";
        boolean isPublicNetwork = true;

        // 公网场景使用私钥签名
        String signature = CryptoUtil.generateSignature(data, rsaConfig.getPublicKey(), rsaConfig.getPrivateKey(), null, isPublicNetwork);
        Assert.assertNotNull("签名不应为空", signature);

        // 公网场景使用公钥验证
        boolean isValid = CryptoUtil.verifySignature(data, signature, rsaConfig.getPublicKey(), null, isPublicNetwork);
        Assert.assertTrue("签名验证应通过", isValid);

        // 验证错误签名
        boolean isInvalid = CryptoUtil.verifySignature(data, "wrong-signature", rsaConfig.getPublicKey(), null, isPublicNetwork);
        Assert.assertFalse("错误签名应验证失败", isInvalid);

        logger.info("公网场景签名测试通过");
    }

    @Test
    public void testSignature_PrivateNetwork() {
        logger.info("=== 测试签名（内网场景） ===");
        String data = "test-signature-data";
        String secret = "test-secret-key";
        boolean isPublicNetwork = false;

        // 内网场景使用HMAC密钥签名
        String signature = CryptoUtil.generateSignature(data, null, null, secret, isPublicNetwork);
        Assert.assertNotNull("签名不应为空", signature);

        // 内网场景使用HMAC密钥验证
        boolean isValid = CryptoUtil.verifySignature(data, signature, null, secret, isPublicNetwork);
        Assert.assertTrue("签名验证应通过", isValid);

        // 验证错误签名
        boolean isInvalid = CryptoUtil.verifySignature(data, "wrong-signature", null, secret, isPublicNetwork);
        Assert.assertFalse("错误签名应验证失败", isInvalid);

        logger.info("内网场景签名测试通过");
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

        Map<String, String> aesKeyPair = CryptoUtil.generateAESKeyPair();
        String aesKey = aesKeyPair.get("aesKey");

        long startTime = System.currentTimeMillis();
        Map<String, String> encryptedResult = CryptoUtil.encryptData(largeDataStr, aesKey);
        long encryptTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        String decryptedData = CryptoUtil.decryptData(encryptedResult.get("encryptedData"), encryptedResult.get("iv"), aesKey);
        long decryptTime = System.currentTimeMillis() - startTime;

        Assert.assertEquals("大数据解密后应相同", largeDataStr, decryptedData);
        logger.info("大数据加密耗时 - 加密: {}ms, 解密: {}ms", encryptTime, decryptTime);
    }
}

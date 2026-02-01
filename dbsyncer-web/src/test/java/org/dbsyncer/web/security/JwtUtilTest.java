/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * JWT工具类测试
 * 
 * @author 穿云
 * @version 1.0.0
 */
public class JwtUtilTest {

    private static final Logger logger = LoggerFactory.getLogger(JwtUtilTest.class);

    private static final String TEST_SECRET = "test-secret-key-123456789012345678901234567890";

    @Test
    public void testGenerateToken() throws NoSuchAlgorithmException, InvalidKeyException {
        logger.info("=== 测试生成Token ===");
        String token = JwtUtil.generateToken(TEST_SECRET);
        Assert.assertNotNull("Token不应为空", token);
        Assert.assertEquals("Token应包含两个点", 3, token.split("\\.").length);
        logger.info("生成的Token: {}", token);
    }

    @Test
    public void testVerifyToken() throws NoSuchAlgorithmException, InvalidKeyException {
        logger.info("=== 测试验证Token ===");
        String token = JwtUtil.generateToken(TEST_SECRET);
        
        // 验证有效Token
        JwtUtil.TokenInfo tokenInfo = JwtUtil.verifyToken(token, TEST_SECRET);
        Assert.assertNotNull("Token信息不应为空", tokenInfo);
        Assert.assertTrue("Token应有效", tokenInfo.isValid());
        
        // 验证错误密钥
        JwtUtil.TokenInfo invalidTokenInfo = JwtUtil.verifyToken(token, "wrong-secret");
        Assert.assertNull("错误密钥应返回null", invalidTokenInfo);
        
        // 验证null Token
        JwtUtil.TokenInfo nullTokenInfo = JwtUtil.verifyToken(null, TEST_SECRET);
        Assert.assertNull("null Token应返回null", nullTokenInfo);
        logger.info("Token验证测试通过");
    }

    @Test
    public void testRefreshToken() throws InterruptedException, NoSuchAlgorithmException, InvalidKeyException {
        logger.info("=== 测试刷新Token ===");
        String originalToken = JwtUtil.generateToken(TEST_SECRET);
        
        // 添加小延迟，确保时间戳不同
        Thread.sleep(10);
        
        // 立即刷新（在刷新窗口内）
        String refreshedToken = JwtUtil.refreshToken(originalToken, TEST_SECRET);
        Assert.assertNotNull("刷新Token不应为空", refreshedToken);
        
        // 验证新Token
        JwtUtil.TokenInfo newTokenInfo = JwtUtil.verifyToken(refreshedToken, TEST_SECRET);
        Assert.assertNotNull("新Token应有效", newTokenInfo);
        Assert.assertTrue("新Token应有效", newTokenInfo.isValid());
        
        // 验证新Token的过期时间应该更新（比原Token晚）
        JwtUtil.TokenInfo originalTokenInfo = JwtUtil.verifyToken(originalToken, TEST_SECRET);
        assert originalTokenInfo != null;
        Assert.assertTrue("新Token的过期时间应晚于原Token",
                newTokenInfo.getExp() > originalTokenInfo.getExp());
        
        logger.info("Token刷新成功，原Token过期时间: {}, 新Token过期时间: {}", 
                originalTokenInfo.getExp(), newTokenInfo.getExp());
    }

    @Test
    public void testTokenExpiration() throws NoSuchAlgorithmException, InvalidKeyException {
        logger.info("=== 测试Token过期 ===");
        String token = JwtUtil.generateToken(TEST_SECRET);
        JwtUtil.TokenInfo tokenInfo = JwtUtil.verifyToken(token, TEST_SECRET);
        Assert.assertNotNull("Token应有效", tokenInfo);
        Assert.assertTrue("Token应有效", tokenInfo.isValid());
        logger.info("Token过期时间: {}", tokenInfo.getExp());
    }

    @Test
    public void testTokenWithDifferentSecrets() throws NoSuchAlgorithmException, InvalidKeyException {
        logger.info("=== 测试不同密钥的Token ===");
        String secret1 = "secret-key-1-123456789012345678901234567890";
        String secret2 = "secret-key-2-123456789012345678901234567890";
        
        String token1 = JwtUtil.generateToken(secret1);
        String token2 = JwtUtil.generateToken(secret2);
        
        // 验证各自密钥
        JwtUtil.TokenInfo info1 = JwtUtil.verifyToken(token1, secret1);
        JwtUtil.TokenInfo info2 = JwtUtil.verifyToken(token2, secret2);
        Assert.assertNotNull("Token1应能用secret1验证", info1);
        Assert.assertNotNull("Token2应能用secret2验证", info2);
        
        // 验证交叉验证应失败
        Assert.assertNull("Token1不能用secret2验证", JwtUtil.verifyToken(token1, secret2));
        Assert.assertNull("Token2不能用secret1验证", JwtUtil.verifyToken(token2, secret1));
        
        logger.info("不同密钥Token测试通过");
    }
}

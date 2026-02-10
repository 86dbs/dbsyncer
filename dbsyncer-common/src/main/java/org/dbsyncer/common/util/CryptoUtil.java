/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.dbsyncer.common.model.AesData;
import org.dbsyncer.common.model.OpenApiData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;

/**
 * 加密工具类
 * 支持AES-GCM加密和RSA密钥交换
 * 支持公网/内网场景的签名算法
 *
 * @author AE86
 * @version 2.0.0
 */
public final class CryptoUtil {

    private static final Logger logger = LoggerFactory.getLogger(CryptoUtil.class);

    // AES-GCM配置
    private static final String AES_ALGORITHM = "AES/GCM/NoPadding";
    private static final String KEY_ALGORITHM = "AES";
    private static final int AES_KEY_SIZE = 256;
    private static final int GCM_TAG_LENGTH = 16;
    private static final int GCM_IV_LENGTH = 12;

    /**
     * 生成AES密钥对
     */
    public static String generateAESKeyPair() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance(KEY_ALGORITHM);
            keyGenerator.init(AES_KEY_SIZE);
            SecretKey secretKey = keyGenerator.generateKey();

            return Base64.getEncoder().encodeToString(secretKey.getEncoded());
        } catch (Exception e) {
            logger.error("生成AES密钥失败", e);
            throw new RuntimeException("生成AES密钥失败", e);
        }
    }

    /**
     * AES-GCM加密数据
     */
    public static AesData encryptData(String data, String aesKey) {
        try {
            SecureRandom random = new SecureRandom();
            byte[] iv = new byte[GCM_IV_LENGTH];
            random.nextBytes(iv);

            SecretKeySpec keySpec = new SecretKeySpec(Base64.getDecoder().decode(aesKey), KEY_ALGORITHM);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);

            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmParameterSpec);

            byte[] encryptedData = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));

            AesData aesData = new AesData();
            aesData.setEncryptedData(Base64.getEncoder().encodeToString(encryptedData));
            aesData.setIv(Base64.getEncoder().encodeToString(iv));

            return aesData;
        } catch (Exception e) {
            logger.error("AES加密数据失败", e);
            throw new RuntimeException("AES加密数据失败", e);
        }
    }

    /**
     * AES-GCM解密数据
     */
    public static String decryptData(String encryptedData, String iv, String aesKey) {
        try {
            SecretKeySpec keySpec = new SecretKeySpec(Base64.getDecoder().decode(aesKey), KEY_ALGORITHM);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, Base64.getDecoder().decode(iv));

            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmParameterSpec);

            byte[] decryptedData = cipher.doFinal(Base64.getDecoder().decode(encryptedData));
            return new String(decryptedData, StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error("AES解密数据失败", e);
            throw new RuntimeException("AES解密数据失败", e);
        }
    }

    /**
     * 构建加密传输请求
     *
     * @param data            要加密的数据
     * @param privateKey      私钥
     * @param hmacSecret      HMAC密钥（用于签名，内网场景必需）
     * @param isPublicNetwork 是否为公网场景（true-公网使用RSA-SHA256签名，false-内网使用HMAC-SHA256签名）
     * @return 加密后的请求JSON字符串
     */
    public static OpenApiData encryptResponse(String data, RSAPrivateKey privateKey, String hmacSecret, boolean isPublicNetwork) {
        // 1. 生成临时AES密钥
        String aesKey = generateAESKeyPair();

        // 2. AES加密数据
        AesData aesData = encryptData(data, aesKey);

        // 3. RSA加密AES密钥
        String encryptedAESKey = RSAUtil.privateEncrypt(aesKey, privateKey);

        // 4. 构建传输结构
        OpenApiData request = new OpenApiData();
        request.setTimestamp(System.currentTimeMillis());
        request.setNonce(CryptoUtil.generateNonce());
        request.setEncryptedKey(encryptedAESKey);
        request.setEncryptedData(aesData.getEncryptedData());
        request.setIv(aesData.getIv());

        // 5. 生成签名（根据场景选择不同算法）
        String dataToSign = buildUnifiedSignData(request.getEncryptedData(), request.getTimestamp(), request.getNonce());
        String signature = isPublicNetwork ? RSAUtil.signSHA256(dataToSign, privateKey) : hmacSha256Sign(dataToSign, hmacSecret);
        logger.info("签名:{}", signature);
        request.setSignature(signature);
        return request;
    }

    /**
     * 解析加密传输请求 - 服务端使用
     *
     * @param data            加密数据
     * @param publicKey       公钥
     * @param hmacSecret      HMAC密钥（用于验证签名，内网场景必需）
     * @param isPublicNetwork 是否为公网场景
     * @return 解密数据
     */
    public static String decryptRequest(OpenApiData data, RSAPublicKey publicKey, String hmacSecret, boolean isPublicNetwork) {
        String signature = data.getSignature();
        if (signature == null || signature.isEmpty()) {
            throw new RuntimeException("签名不能为空");
        }

        // 1. 验证签名
        String dataToVerify = buildUnifiedSignData(data.getEncryptedData(), data.getTimestamp(), data.getNonce());
        boolean verifySignature = isPublicNetwork ? RSAUtil.verifySHA256(dataToVerify, signature, publicKey) : signature.equals(hmacSha256Sign(dataToVerify, hmacSecret));
        if (!verifySignature) {
            throw new RuntimeException("签名验证失败");
        }

        String iv = data.getIv();
        if (iv == null || iv.isEmpty()) {
            throw new RuntimeException("IV不能为空");
        }
        // 2. RSA解密AES密钥
        String aesKey = RSAUtil.publicDecrypt(data.getEncryptedKey(), publicKey);

        // 3. AES解密数据
        return decryptData(data.getEncryptedData(), iv, aesKey);
    }

    /**
     * 统一的签名字符串构建方法
     * 加密和解鉴必须使用同一个方法
     */
    private static String buildUnifiedSignData(String encryptedData, long timestamp, String nonce) {
        return String.format("%s|%d|%s", encryptedData, timestamp, nonce);
    }

    /**
     * 生成随机nonce
     */
    private static String generateNonce() {
        SecureRandom random = new SecureRandom();
        byte[] nonce = new byte[16];
        random.nextBytes(nonce);
        return Base64.getEncoder().encodeToString(nonce);
    }

    /**
     * HMAC-SHA256签名（内网场景）
     */
    private static String hmacSha256Sign(String data, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hash = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            logger.error("HMAC-SHA256签名失败", e);
            throw new RuntimeException("HMAC-SHA256签名失败", e);
        }
    }

}

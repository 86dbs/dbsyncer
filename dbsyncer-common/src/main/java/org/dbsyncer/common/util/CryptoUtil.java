package org.dbsyncer.common.util;

import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class CryptoUtil {

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
    public static Map<String, String> generateAESKeyPair() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance(KEY_ALGORITHM);
            keyGenerator.init(AES_KEY_SIZE);
            SecretKey secretKey = keyGenerator.generateKey();

            Map<String, String> keyMap = new HashMap<>();
            keyMap.put("aesKey", Base64.getEncoder().encodeToString(secretKey.getEncoded()));
            return keyMap;
        } catch (Exception e) {
            logger.error("生成AES密钥失败", e);
            throw new RuntimeException("生成AES密钥失败", e);
        }
    }

    /**
     * 使用RSA公钥加密AES密钥
     */
    public static String encryptAESKey(String aesKey, String rsaPublicKey) {
        try {
            return RSAUtil.publicEncrypt(aesKey, RSAUtil.getPublicKey(rsaPublicKey));
        } catch (Exception e) {
            logger.error("RSA加密AES密钥失败", e);
            throw new RuntimeException("RSA加密AES密钥失败", e);
        }
    }

    /**
     * 使用RSA私钥解密AES密钥
     */
    public static String decryptAESKey(String encryptedAESKey, String rsaPrivateKey) {
        try {
            return RSAUtil.privateDecrypt(encryptedAESKey, RSAUtil.getPrivateKey(rsaPrivateKey));
        } catch (Exception e) {
            logger.error("RSA解密AES密钥失败", e);
            throw new RuntimeException("RSA解密AES密钥失败", e);
        }
    }

    /**
     * AES-GCM加密数据
     */
    public static Map<String, String> encryptData(String data, String aesKey) {
        try {
            SecureRandom random = new SecureRandom();
            byte[] iv = new byte[GCM_IV_LENGTH];
            random.nextBytes(iv);

            SecretKeySpec keySpec = new SecretKeySpec(Base64.getDecoder().decode(aesKey), KEY_ALGORITHM);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);

            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmParameterSpec);

            byte[] encryptedData = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));

            Map<String, String> result = new HashMap<>();
            result.put("encryptedData", Base64.getEncoder().encodeToString(encryptedData));
            result.put("iv", Base64.getEncoder().encodeToString(iv));

            return result;
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
     * 构建加密传输请求 - 客户端使用
     */
    public static String buildEncryptedRequest(Object data, String rsaPublicKey) {
        try {
            // 1. 生成临时AES密钥
            Map<String, String> aesKeyPair = generateAESKeyPair();
            String aesKey = aesKeyPair.get("aesKey");

            // 2. 序列化数据
            String jsonData = JsonUtil.objToJson(data);

            // 3. AES加密数据
            Map<String, String> encryptedResult = encryptData(jsonData, aesKey);

            // 4. RSA加密AES密钥
            String encryptedAESKey = encryptAESKey(aesKey, rsaPublicKey);

            // 5. 构建传输结构
            JSONObject request = new JSONObject();
            request.put("timestamp", System.currentTimeMillis());
            request.put("nonce", generateNonce());
            request.put("encryptedData", encryptedResult.get("encryptedData"));
            request.put("iv", encryptedResult.get("iv"));
            request.put("encryptedKey", encryptedAESKey);

            // 6. 生成签名
            String signature = generateSignature(request.toJSONString(), rsaPublicKey);
            request.put("signature", signature);

            return request.toJSONString();
        } catch (Exception e) {
            logger.error("构建加密请求失败", e);
            throw new RuntimeException("构建加密请求失败", e);
        }
    }

    /**
     * 解析加密传输请求 - 服务端使用
     */
    public static <T> T parseEncryptedRequest(String encryptedRequest, String rsaPrivateKey, Class<T> clazz) {
        try {
            JSONObject request = JsonUtil.jsonToObj(encryptedRequest, JSONObject.class);

            // 1. 验证签名 (省略具体实现，实际项目中应实现)

            // 2. RSA解密AES密钥
            String encryptedKey = request.getString("encryptedKey");
            String aesKey = decryptAESKey(encryptedKey, rsaPrivateKey);

            // 3. AES解密数据
            String encryptedData = request.getString("encryptedData");
            String iv = request.getString("iv");
            String jsonData = decryptData(encryptedData, iv, aesKey);

            // 4. 反序列化数据
            return JsonUtil.jsonToObj(jsonData, clazz);
        } catch (Exception e) {
            logger.error("解析加密请求失败", e);
            throw new RuntimeException("解析加密请求失败", e);
        }
    }

    /**
     * 构建加密响应 - 服务端使用
     */
    public static String buildEncryptedResponse(Object data, String rsaPublicKey) {
        return buildEncryptedRequest(data, rsaPublicKey);
    }

    /**
     * 解析加密响应 - 客户端使用
     */
    public static <T> T parseEncryptedResponse(String encryptedResponse, String rsaPrivateKey, Class<T> clazz) {
        return parseEncryptedRequest(encryptedResponse, rsaPrivateKey, clazz);
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
     * 生成签名 (简化实现，实际应使用更安全的签名算法)
     * - 内部系统：HMAC-SHA256
     * - 公网系统：RSA-SHA256安全签名 高安全要求
     */
    private static String generateSignature(String data, String publicKey) {
        try {
            // 这里简化实现，实际项目中应使用RSA-SHA256等更安全的签名算法
            return CheckSumUtil.hmacSha1(data, publicKey.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("生成签名失败", e);
            throw new RuntimeException("生成签名失败", e);
        }
    }

}
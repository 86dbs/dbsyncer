package org.dbsyncer.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson2.JSONObject;

import com.alibaba.fastjson2.JSONObject;

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
import java.util.HashMap;
import java.util.Map;

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
            RSAPublicKey publicKey = RSAUtil.getPublicKey(rsaPublicKey);
            return RSAUtil.publicEncrypt(aesKey, publicKey);
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
            RSAPrivateKey privateKey = RSAUtil.getPrivateKey(rsaPrivateKey);
            return RSAUtil.privateDecrypt(encryptedAESKey, privateKey);
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
     * 
     * @param data 要加密的数据
     * @param rsaPublicKey RSA公钥（用于加密AES密钥）
     * @param rsaPrivateKey RSA私钥（用于签名，公网场景必需）
     * @param hmacSecret HMAC密钥（用于签名，内网场景必需）
     * @param isPublicNetwork 是否为公网场景（true-公网使用RSA-SHA256签名，false-内网使用HMAC-SHA256签名）
     * @return 加密后的请求JSON字符串
     */
    public static String buildEncryptedRequest(Object data, String rsaPublicKey, String rsaPrivateKey, String hmacSecret, boolean isPublicNetwork) {
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

            // 6. 生成签名（根据场景选择不同算法）
            String dataToSign = request.toJSONString();
            String signature = generateSignature(dataToSign, rsaPublicKey, rsaPrivateKey, hmacSecret, isPublicNetwork);
            request.put("signature", signature);

            return request.toJSONString();
        } catch (Exception e) {
            logger.error("构建加密请求失败", e);
            throw new RuntimeException("构建加密请求失败", e);
        }
    }

    /**
     * 解析加密传输请求 - 服务端使用
     * 
     * @param encryptedRequest 加密的请求JSON字符串
     * @param rsaPrivateKey RSA私钥（用于解密AES密钥）
     * @param rsaPublicKey RSA公钥（用于验证签名，公网场景必需）
     * @param hmacSecret HMAC密钥（用于验证签名，内网场景必需）
     * @param isPublicNetwork 是否为公网场景
     * @param clazz 目标类型
     * @return 解密后的数据对象
     */
    public static <T> T parseEncryptedRequest(String encryptedRequest, String rsaPrivateKey, String rsaPublicKey, String hmacSecret, boolean isPublicNetwork, Class<T> clazz) {
        try {
            JSONObject request = JsonUtil.jsonToObj(encryptedRequest, JSONObject.class);

            // 1. 验证签名
            String signature = request.getString("signature");
            if (signature == null || signature.isEmpty()) {
                throw new RuntimeException("签名不能为空");
            }

            // 创建副本用于验证签名（不包含signature字段）
            JSONObject requestForVerify = new JSONObject(request);
            requestForVerify.remove("signature");
            String dataToVerify = requestForVerify.toJSONString();

            boolean signatureValid = false;
            if (isPublicNetwork) {
                if (rsaPublicKey == null || rsaPublicKey.isEmpty()) {
                    throw new RuntimeException("公网场景需要RSA公钥进行签名验证");
                }
                signatureValid = verifySignature(dataToVerify, signature, rsaPublicKey, null, true);
            } else {
                if (hmacSecret == null || hmacSecret.isEmpty()) {
                    throw new RuntimeException("内网场景需要HMAC密钥进行签名验证");
                }
                signatureValid = verifySignature(dataToVerify, signature, null, hmacSecret, false);
            }

            if (!signatureValid) {
                throw new RuntimeException("签名验证失败");
            }

            // 2. RSA解密AES密钥
            String encryptedKey = request.getString("encryptedKey");
            if (encryptedKey == null || encryptedKey.isEmpty()) {
                throw new RuntimeException("加密的AES密钥不能为空");
            }
            String aesKey = decryptAESKey(encryptedKey, rsaPrivateKey);

            // 3. AES解密数据
            String encryptedData = request.getString("encryptedData");
            String iv = request.getString("iv");
            if (encryptedData == null || encryptedData.isEmpty()) {
                throw new RuntimeException("加密数据不能为空");
            }
            if (iv == null || iv.isEmpty()) {
                throw new RuntimeException("IV不能为空");
            }

            String jsonData = decryptData(encryptedData, iv, aesKey);

            if (StringUtil.isBlank(jsonData)) {
                throw new RuntimeException("解密后的数据为空");
            }

            logger.debug("解密后的JSON数据: {}", jsonData);

            // 4. 反序列化数据
            return JsonUtil.jsonToObj(jsonData, clazz);
        } catch (RuntimeException e) {
            logger.error("解析加密请求失败: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("解析加密请求失败", e);
            throw new RuntimeException("解析加密请求失败: " + e.getMessage(), e);
        }
    }

    /**
     * 构建加密响应 - 服务端使用
     * 
     * @param data 要加密的数据
     * @param rsaPublicKey RSA公钥（用于加密AES密钥）
     * @param rsaPrivateKey RSA私钥（用于签名，公网场景必需）
     * @param hmacSecret HMAC密钥（用于签名，内网场景必需）
     * @param isPublicNetwork 是否为公网场景
     * @return 加密后的响应JSON字符串
     */
    public static String buildEncryptedResponse(Object data, String rsaPublicKey, String rsaPrivateKey, String hmacSecret, boolean isPublicNetwork) {
        return buildEncryptedRequest(data, rsaPublicKey, rsaPrivateKey, hmacSecret, isPublicNetwork);
    }

    /**
     * 解析加密响应 - 客户端使用
     * 
     * @param encryptedResponse 加密的响应JSON字符串
     * @param rsaPrivateKey RSA私钥（用于解密AES密钥）
     * @param rsaPublicKey RSA公钥（用于验证签名，公网场景必需）
     * @param hmacSecret HMAC密钥（用于验证签名，内网场景必需）
     * @param isPublicNetwork 是否为公网场景
     * @param clazz 目标类型
     * @return 解密后的数据对象
     */
    public static <T> T parseEncryptedResponse(String encryptedResponse, String rsaPrivateKey, String rsaPublicKey, String hmacSecret, boolean isPublicNetwork, Class<T> clazz) {
        return parseEncryptedRequest(encryptedResponse, rsaPrivateKey, rsaPublicKey, hmacSecret, isPublicNetwork, clazz);
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
     * 生成签名
     * - 公网场景：使用RSA-SHA256签名（使用私钥签名）
     * - 内网场景：使用HMAC-SHA256签名（性能更好）
     * 
     * @param data 要签名的数据
     * @param rsaPublicKey RSA公钥（公网场景，用于标识，实际签名使用私钥）
     * @param rsaPrivateKey RSA私钥（公网场景，用于签名）
     * @param hmacSecret HMAC密钥（内网场景）
     * @param isPublicNetwork 是否为公网场景
     * @return 签名
     */
    public static String generateSignature(String data, String rsaPublicKey, String rsaPrivateKey, String hmacSecret, boolean isPublicNetwork) {
        try {
            if (isPublicNetwork) {
                // 公网场景：使用RSA-SHA256签名（使用私钥签名）
                if (rsaPrivateKey == null || rsaPrivateKey.isEmpty()) {
                    throw new RuntimeException("公网场景需要RSA私钥进行签名");
                }
                return RSAUtil.signSHA256(data, rsaPrivateKey);
            } else {
                // 内网场景：使用HMAC-SHA256签名（性能更好）
                if (hmacSecret == null || hmacSecret.isEmpty()) {
                    throw new RuntimeException("内网场景需要HMAC密钥进行签名");
                }
                return hmacSha256Sign(data, hmacSecret);
            }
        } catch (Exception e) {
            logger.error("生成签名失败", e);
            throw new RuntimeException("生成签名失败: " + e.getMessage(), e);
        }
    }

    /**
     * 验证签名
     * 
     * @param data 原始数据
     * @param signature 签名
     * @param rsaPublicKey RSA公钥（公网场景，用于验证签名）
     * @param hmacSecret HMAC密钥（内网场景，用于验证签名）
     * @param isPublicNetwork 是否为公网场景
     * @return 验证结果
     */
    public static boolean verifySignature(String data, String signature, String rsaPublicKey, String hmacSecret, boolean isPublicNetwork) {
        try {
            if (isPublicNetwork) {
                // 公网场景：使用RSA-SHA256验证
                if (rsaPublicKey == null || rsaPublicKey.isEmpty()) {
                    logger.error("公网场景需要RSA公钥进行签名验证");
                    return false;
                }
                return RSAUtil.verifySHA256(data, signature, rsaPublicKey);
            } else {
                // 内网场景：使用HMAC-SHA256验证
                if (hmacSecret == null || hmacSecret.isEmpty()) {
                    logger.error("内网场景需要HMAC密钥进行签名验证");
                    return false;
                }
                String expectedSignature = hmacSha256Sign(data, hmacSecret);
                return expectedSignature != null && expectedSignature.equals(signature);
            }
        } catch (Exception e) {
            logger.error("验证签名失败", e);
            return false;
        }
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

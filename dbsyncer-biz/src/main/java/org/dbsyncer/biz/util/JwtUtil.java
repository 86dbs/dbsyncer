/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.util;

import org.dbsyncer.biz.model.TokenInfo;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * JWT工具类
 * 支持生成、验证、刷新token
 *
 * @author 穿云
 * @version 1.0.0
 */
public abstract class JwtUtil {

    private static final Logger logger = LoggerFactory.getLogger(JwtUtil.class);

    private static final String ALGORITHM = "HmacSHA256";
    private static final String TYP = "JWT";
    private static final String ALG = "HS256";

    // Token有效期：2小时（毫秒）
    private static final long TOKEN_EXPIRE_TIME = 2 * 60 * 60 * 1000L;

    // Token刷新时间窗口：1.5小时（毫秒），在此时间窗口内可以刷新token
    private static final long TOKEN_REFRESH_TIME = (long) (1.5 * 60 * 60 * 1000);

    /**
     * 生成JWT Token
     *
     * @param secret 密钥
     * @return JWT Token
     */
    public static String generateToken(String secret) throws NoSuchAlgorithmException, InvalidKeyException {
        long now = System.currentTimeMillis();
        long expireTime = now + TOKEN_EXPIRE_TIME;

        // Header
        Map<String, String> header = new HashMap<>();
        header.put("typ", TYP);
        header.put("alg", ALG);
        String headerJson = JsonUtil.objToJson(header);
        String headerBase64 = base64UrlEncode(headerJson.getBytes(StandardCharsets.UTF_8));

        // Payload
        TokenInfo payload = new TokenInfo();
        // 过期时间
        payload.setExp(expireTime);
        // 签发时间
        payload.setIat(now);
        String payloadJson = JsonUtil.objToJson(payload);
        String payloadBase64 = base64UrlEncode(payloadJson.getBytes(StandardCharsets.UTF_8));

        // Signature
        String data = headerBase64 + "." + payloadBase64;
        String signature = hmacSha256(data, secret);
        String signatureBase64 = base64UrlEncode(signature.getBytes(StandardCharsets.UTF_8));
        return data + "." + signatureBase64;
    }

    /**
     * 验证JWT Token
     *
     * @param token  JWT Token
     * @param secret 密钥
     * @return Token信息，验证失败返回null
     */
    public static TokenInfo verifyToken(String token, String secret) throws NoSuchAlgorithmException, InvalidKeyException {
        if (StringUtil.isBlank(token) || StringUtil.isBlank(secret)) {
            return null;
        }

        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            logger.warn("Token格式错误，部分数量: {}", parts.length);
            return null;
        }

        // 验证签名
        String data = parts[0] + "." + parts[1];
        String expectedSignature = base64UrlEncode(hmacSha256(data, secret).getBytes(StandardCharsets.UTF_8));
        if (!expectedSignature.equals(parts[2])) {
            logger.warn("Token签名验证失败");
            return null;
        }

        // 解析Payload
        String payloadJson = new String(base64UrlDecode(parts[1]), StandardCharsets.UTF_8);
        TokenInfo tokenInfo = JsonUtil.jsonToObj(payloadJson, TokenInfo.class);

        // 验证过期时间
        Long exp = tokenInfo.getExp();
        if (exp == null || exp < System.currentTimeMillis()) {
            logger.warn("Token已过期，exp: {}", exp);
            return null;
        }

        return tokenInfo;
    }

    /**
     * 刷新Token
     * 如果token在刷新时间窗口内，可以刷新生成新token
     *
     * @param token  原Token
     * @param secret 密钥
     * @return 新Token，如果不在刷新窗口内返回null
     */
    public static String refreshToken(String token, String secret) throws NoSuchAlgorithmException, InvalidKeyException {
        TokenInfo tokenInfo = verifyToken(token, secret);
        if (tokenInfo == null) {
            return null;
        }

        long now = System.currentTimeMillis();
        long elapsed = now - tokenInfo.getIat();

        // 检查是否在刷新时间窗口内
        if (elapsed > TOKEN_REFRESH_TIME) {
            logger.warn("Token不在刷新时间窗口内，elapsed: {}ms", elapsed);
            return null;
        }

        // 生成新token
        return generateToken(secret);
    }

    /**
     * HMAC-SHA256签名
     */
    private static String hmacSha256(String data, String secret) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = Mac.getInstance(ALGORITHM);
        SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM);
        mac.init(secretKeySpec);
        byte[] hash = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }

    /**
     * Base64 URL安全编码
     */
    private static String base64UrlEncode(byte[] data) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data);
    }

    /**
     * Base64 URL安全解码
     */
    private static byte[] base64UrlDecode(String data) {
        return Base64.getUrlDecoder().decode(data);
    }

}

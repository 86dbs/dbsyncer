package org.dbsyncer.common.util;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;

public class CheckSumUtil {

    private static final String HEX_DIGITS = "0123456789abcdef";

    /**
     * hmac_sha1算法计算内容摘要
     * @param value 原始数据
     * @param keyBytes 密钥
     * @return
     */
    public static String hmacSha1(String value, byte[] keyBytes) {
        try {
            SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA1");
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(signingKey);
            byte[] rawHmac = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
            String digest = toHexString(rawHmac);
            return digest;
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     *二进制字节数组转十六进制
     * @param v
     * @return
     */
    public static String toHexString(byte[] v) {
        StringBuilder sb = new StringBuilder(v.length * 2);
        for (byte value : v) {
            int b = value & 0xFF;
            sb.append(HEX_DIGITS.charAt(b >>> 4)).append(HEX_DIGITS.charAt(b & 0xF));
        }
        return sb.toString();
    }
}

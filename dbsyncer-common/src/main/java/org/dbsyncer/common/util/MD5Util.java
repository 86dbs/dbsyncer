package org.dbsyncer.common.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * MD5加密工具类
 */
public class MD5Util {
    // MD5算法名称
    private static final String MD5_ALGORITHM = "MD5";
    // 十六进制字符集
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /**
     * 对字符串进行MD5加密
     *
     * @param input 待加密的字符串
     * @return 加密后的十六进制字符串
     */
    public static String encrypt(String input) {
        if (input == null) {
            return null;
        }

        try {
            // 获取MD5消息摘要实例
            MessageDigest md = MessageDigest.getInstance(MD5_ALGORITHM);
            
            // 计算哈希值（字节数组）
            byte[] hashBytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            
            // 转换为十六进制字符串
            return bytesToHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            // 理论上不会抛出此异常，因为MD5是标准算法
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    /**
     * 对文件进行MD5加密
     *
     * @param filePath 文件路径
     * @return 文件的MD5值（十六进制字符串）
     * @throws IOException 如果文件读取失败
     */
    public static String encryptFile(String filePath) throws IOException {
        try (FileInputStream fis = new FileInputStream(filePath);
             DigestInputStream dis = new DigestInputStream(fis, MessageDigest.getInstance(MD5_ALGORITHM))) {
            
            // 读取文件内容以更新摘要
            byte[] buffer = new byte[8192];
            while (dis.read(buffer) != -1) {
                // 持续读取直到文件结束，DigestInputStream会自动更新摘要
            }
            
            // 获取最终的哈希值
            byte[] hashBytes = dis.getMessageDigest().digest();
            return bytesToHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    /**
     * 将字节数组转换为十六进制字符串
     *
     * @param bytes 字节数组
     * @return 十六进制字符串
     */
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        char[] hexChars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            // 取字节的高4位
            int high = (bytes[i] >> 4) & 0x0F;
            // 取字节的低4位
            int low = bytes[i] & 0x0F;

            hexChars[i * 2] = HEX_CHARS[high];
            hexChars[i * 2 + 1] = HEX_CHARS[low];
        }
        return new String(hexChars);
    }

    // 测试方法
    public static void main(String[] args) {
        // 测试字符串加密

        Map<String, String> params = new HashMap<>();
        params.put("a", "1");
        params.put("b", "2");
        params.put("c", "3");


        Map<String, String> params2 = new HashMap<>();
        params2.put("a", "1");
        params2.put("b", "2");
        params2.put("c", "3");

        System.out.println(encrypt(params.toString()));
        System.out.println(encrypt(params2.toString()));

    }
}
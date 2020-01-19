package org.dbsyncer.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

public abstract class AESEncyptUtil {
    
    private final static Logger logger = LoggerFactory.getLogger(AESEncyptUtil.class);

    // 默认缺省种子
    public static String keySeed = "c32ad1415f6c89fee76d8457c31efb4b";

    /**
     * 加密
     * 
     * @param content
     *            需要加密的内容
     * @param keyseed
     *            密钥
     * @return
     */
    public static String encrypt(String content, String keyseed) throws Exception {
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        // 解决linux环境下密码解密问题
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(keyseed.getBytes("UTF-8"));
        kgen.init(128, secureRandom);
        SecretKey secretKey = kgen.generateKey();
        byte[] enCodeFormat = secretKey.getEncoded();
        SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
        Cipher cipher = Cipher.getInstance("AES");// 创建密码器
        byte[] byteContent = content.getBytes("utf-8");
        cipher.init(Cipher.ENCRYPT_MODE, key);// 初始化
        byte[] result = cipher.doFinal(byteContent);
        return byte2Hex(result); // 加密
    }

    /**
     * 解密
     * 
     * @param content
     *            待解密内容
     * @param keyseed
     *            解密密钥
     * @return
     */
    public static byte[] decrypt(byte[] content, String keyseed) throws Exception {
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        // 解决linux环境下密码解密问题
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(keyseed.getBytes("UTF-8"));
        kgen.init(128, secureRandom);
        SecretKey secretKey = kgen.generateKey();
        byte[] enCodeFormat = secretKey.getEncoded();
        SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
        Cipher cipher = Cipher.getInstance("AES");// 创建密码器
        cipher.init(Cipher.DECRYPT_MODE, key);// 初始化
        byte[] result = cipher.doFinal(hex2Byte(content));
        return result; // 加密
    }

    /**
     * 解密
     * 
     * @param content
     *            待解密内容
     * @param keyseed
     *            解密密钥
     * @return
     */
    public static String decrypt(String content, String keyseed) throws Exception {
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        // 解决linux环境下密码解密问题
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(keyseed.getBytes("UTF-8"));
        kgen.init(128, secureRandom);
        SecretKey secretKey = kgen.generateKey();
        byte[] enCodeFormat = secretKey.getEncoded();
        SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
        Cipher cipher = Cipher.getInstance("AES");// 创建密码器
        cipher.init(Cipher.DECRYPT_MODE, key);// 初始化
        String result = new String(cipher.doFinal(hex2Byte(content.getBytes("UTF-8"))), "UTF-8");
        return result.trim();
    }

    /**
     * 对于js加密
     * 
     * @param content
     *            内容
     * @param keyseed
     *            解密密钥
     * @return
     * @throws Exception
     */
    public static String encryptForJS(String content, String keyseed) throws Exception {
        SecretKeySpec key = getKeySpecFromBytes(keyseed.toUpperCase());
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] byteEnc = cipher.doFinal(content.getBytes("UTF-8"));
        return byte2Hex(byteEnc);
    }

    /**
     * 解密
     * 
     * @param content
     *            待解密内容
     * @param keyseed
     *            解密密钥
     * @return
     */
    public static String decryptForJS(String content, String keyseed) throws Exception {
        SecretKeySpec key = getKeySpecFromBytes(keyseed.toUpperCase());
        Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, key);
        String result = new String(cipher.doFinal(hex2Byte(content.getBytes("UTF-8"))), "UTF-8");
        return result.trim();

    }

    private static String byte2Hex(byte[] b) {
        String hs = "";
        String stmp = "";
        for (int n = 0; n < b.length; n++) {
            stmp = Integer.toHexString(b[n] & 0xFF);
            if (stmp.length() == 1) {
                hs += "0" + stmp;
            } else {
                hs += stmp;
            }
        }
        return hs.toUpperCase();
    }

    /**
     * 十六进制字符串到字节转换
     * 
     * @param b
     *            byte类型的数组
     * @return
     */
    private static byte[] hex2Byte(byte[] b) {
        if ((b.length % 2) != 0) {
            throw new IllegalArgumentException("长度不是偶数!");
        }
        byte[] b2 = new byte[b.length / 2];

        for (int n = 0; n < b.length; n += 2) {
            try {
                String item = null;
                // 判断n值是否在b字节长度范围之内，否则，造成堆内存溢出
                if (n + 2 <= b.length) {
                    item = new String(b, n, 2, "UTF-8");
                    b2[n / 2] = (byte) Integer.parseInt(item, 16);
                }
                item = "";
                item = null;
            } catch (UnsupportedEncodingException e) {
                logger.error(e.getMessage(), e);
            }
        }
        // 在垃圾回收延迟的情况下，进行记忆清楚，避免信息被窃取
        byte temp = 0;
        // 将字节数组赋值为0，删除原有数据
        Arrays.fill(b, temp);
        return b2;
    }

    /**
     * 从十六进制字符串生成Key
     * 
     * @param strBytes
     *            str字节
     * @return
     * @throws NoSuchAlgorithmException
     */
    private static SecretKeySpec getKeySpecFromBytes(String strBytes) throws NoSuchAlgorithmException {
        SecretKeySpec spec = null;
        try {
            spec = new SecretKeySpec(hex2Byte(strBytes.getBytes("UTF-8")), "AES");
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
        }
        try {
            spec = new SecretKeySpec(hex2Byte(strBytes.getBytes("UTF-8")), "AES");
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
        }
        return spec;
    }

}

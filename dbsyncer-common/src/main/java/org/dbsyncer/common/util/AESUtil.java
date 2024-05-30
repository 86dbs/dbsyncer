/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.dbsyncer.common.CommonException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

public abstract class AESUtil {

    private static KeyGenerator keyGenerator;

    static {
        try {
            keyGenerator = KeyGenerator.getInstance("AES");
        } catch (NoSuchAlgorithmException e) {
            throw new CommonException(e);
        }
    }

    public static String encrypt(String content, String key) {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, getSecretKeySpec(key));
            byte[] result = cipher.doFinal(content.getBytes("utf-8"));
            return byte2Hex(result);
        } catch (Exception e) {
            throw new CommonException(e);
        }
    }

    public static String decrypt(String content, String key) {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, getSecretKeySpec(key));
            String result = new String(cipher.doFinal(hex2Byte(content.getBytes("UTF-8"))), "UTF-8");
            return result.trim();
        } catch (Exception e) {
            throw new CommonException(e);
        }
    }

    private static SecretKeySpec getSecretKeySpec(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        // 解决linux环境下密码解密问题
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(key.getBytes("UTF-8"));
        keyGenerator.init(128, secureRandom);
        byte[] enCodeFormat = keyGenerator.generateKey().getEncoded();
        return new SecretKeySpec(enCodeFormat, "AES");
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
                item = null;
            } catch (UnsupportedEncodingException e) {
                throw new CommonException(e);
            }
        }
        byte temp = 0;
        // 将字节数组赋值为0，删除原有数据
        Arrays.fill(b, temp);
        return b2;
    }

}
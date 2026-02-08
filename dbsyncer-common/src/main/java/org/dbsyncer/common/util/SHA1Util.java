package org.dbsyncer.common.util;

import org.dbsyncer.common.CommonException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public abstract class SHA1Util {

    private static MessageDigest instance;

    static {
        try {
            instance = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            throw new CommonException(e);
        }
    }

    public static String b64_sha1(String s) {
        if (null == s || "" == s.trim()) {
            return null;
        }
        if (null == instance) {
            return null;
        }
        byte[] sha1 = instance.digest(s.getBytes());
        if (null == sha1) {
            return null;
        }
        // base64加密
        return Base64.getEncoder().encodeToString(sha1);
    }

    // public static void main(String[] args) throws Exception {
    // // QL0AFWMIX8NRZTKeof9cXsvbvu8=
    // String data = "123";
    // String b64_sha1 = b64_sha1(data);
    // System.out.println("b64_sha1(" + data + ")=" + b64_sha1);
    // System.out.println("b64_sha1(" + data + ")=" + b64_sha1.length());
    // }
}

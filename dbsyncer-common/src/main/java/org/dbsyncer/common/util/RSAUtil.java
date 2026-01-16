package org.dbsyncer.common.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.model.RSAConfig;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public class RSAUtil {
    public static final String CHARSET = "UTF-8";

    public static final String RSA_ALGORITHM = "RSA";

    public static RSAConfig createKeys(int keySize) {
        //为RSA算法创建一个KeyPairGenerator对象
        KeyPairGenerator kpg;
        try {
            kpg = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("No such algorithm-->[" + RSA_ALGORITHM + "]");
        }

        //初始化KeyPairGenerator对象,密钥长度
        kpg.initialize(keySize);
        //生成密匙对
        KeyPair keyPair = kpg.generateKeyPair();
        //得到公钥
        Key publicKey = keyPair.getPublic();
        String publicKeyStr = Base64.encodeBase64URLSafeString(publicKey.getEncoded());
        //得到私钥
        Key privateKey = keyPair.getPrivate();
        String privateKeyStr = Base64.encodeBase64URLSafeString(privateKey.getEncoded());
        RSAConfig rsaConfig = new RSAConfig();
        rsaConfig.setKeyLength(keySize);
        rsaConfig.setPublicKey(publicKeyStr);
        rsaConfig.setPrivateKey(privateKeyStr);
        return rsaConfig;
    }

    /**
     * 得到公钥
     *
     * @param publicKey 密钥字符串（经过base64编码）
     * @throws Exception
     */
    public static RSAPublicKey getPublicKey(String publicKey) throws NoSuchAlgorithmException, InvalidKeySpecException {
        //通过X509编码的Key指令获得公钥对象
        KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
        X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(Base64.decodeBase64(publicKey));
        RSAPublicKey key = (RSAPublicKey) keyFactory.generatePublic(x509KeySpec);
        return key;
    }

    /**
     * 得到私钥
     *
     * @param privateKey 密钥字符串（经过base64编码）
     * @throws Exception
     */
    public static RSAPrivateKey getPrivateKey(String privateKey) throws NoSuchAlgorithmException, InvalidKeySpecException {
        //通过PKCS#8编码的Key指令获得私钥对象
        KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
        PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(Base64.decodeBase64(privateKey));
        RSAPrivateKey key = (RSAPrivateKey) keyFactory.generatePrivate(pkcs8KeySpec);
        return key;
    }

    /**
     * 公钥加密
     *
     * @param data
     * @param publicKey
     * @return
     */
    public static String publicEncrypt(String data, RSAPublicKey publicKey) {
        try {
            Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            return Base64.encodeBase64URLSafeString(rsaSplitCodec(cipher, Cipher.ENCRYPT_MODE, data.getBytes(CHARSET), publicKey.getModulus().bitLength()));
        } catch (Exception e) {
            throw new RuntimeException("加密字符串[" + data + "]时遇到异常", e);
        }
    }

    /**
     * 私钥解密
     *
     * @param data
     * @param privateKey
     * @return
     */
    public static String privateDecrypt(String data, RSAPrivateKey privateKey) {
        try {
            Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
            cipher.init(Cipher.DECRYPT_MODE, privateKey);
            return new String(rsaSplitCodec(cipher, Cipher.DECRYPT_MODE, Base64.decodeBase64(data), privateKey.getModulus().bitLength()), CHARSET);
        } catch (Exception e) {
            throw new RuntimeException("解密字符串[" + data + "]时遇到异常", e);
        }
    }

    /**
     * 私钥加密
     *
     * @param data
     * @param privateKey
     * @return
     */
    public static String privateEncrypt(String data, RSAPrivateKey privateKey) {
        try {
            Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, privateKey);
            return Base64.encodeBase64URLSafeString(rsaSplitCodec(cipher, Cipher.ENCRYPT_MODE, data.getBytes(CHARSET), privateKey.getModulus().bitLength()));
        } catch (Exception e) {
            throw new RuntimeException("加密字符串[" + data + "]时遇到异常", e);
        }
    }

    /**
     * 公钥解密
     *
     * @param data
     * @param publicKey
     * @return
     */
    public static String publicDecrypt(String data, RSAPublicKey publicKey) {
        try {
            Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, publicKey);
            return new String(rsaSplitCodec(cipher, Cipher.DECRYPT_MODE, Base64.decodeBase64(data), publicKey.getModulus().bitLength()), CHARSET);
        } catch (Exception e) {
            throw new RuntimeException("解密字符串[" + data + "]时遇到异常", e);
        }
    }

    /**
     * RSA-SHA256签名（使用私钥签名）
     * 
     * @param data 要签名的数据
     * @param privateKey RSA私钥
     * @return Base64编码的签名
     */
    public static String signSHA256(String data, RSAPrivateKey privateKey) {
        try {
            java.security.Signature signature = java.security.Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(data.getBytes(CHARSET));
            byte[] signBytes = signature.sign();
            return Base64.encodeBase64URLSafeString(signBytes);
        } catch (Exception e) {
            throw new RuntimeException("RSA-SHA256签名失败", e);
        }
    }

    /**
     * RSA-SHA256签名（使用私钥字符串）
     * 
     * @param data 要签名的数据
     * @param privateKeyStr Base64编码的私钥字符串
     * @return Base64编码的签名
     */
    public static String signSHA256(String data, String privateKeyStr) {
        try {
            RSAPrivateKey privateKey = getPrivateKey(privateKeyStr);
            return signSHA256(data, privateKey);
        } catch (Exception e) {
            throw new RuntimeException("RSA-SHA256签名失败", e);
        }
    }

    /**
     * RSA-SHA256验证签名（使用公钥验证）
     * 
     * @param data 原始数据
     * @param signature Base64编码的签名
     * @param publicKey RSA公钥
     * @return 验证结果
     */
    public static boolean verifySHA256(String data, String signature, RSAPublicKey publicKey) {
        try {
            java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
            sig.initVerify(publicKey);
            sig.update(data.getBytes(CHARSET));
            byte[] signBytes = Base64.decodeBase64(signature);
            return sig.verify(signBytes);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * RSA-SHA256验证签名（使用公钥字符串）
     * 
     * @param data 原始数据
     * @param signature Base64编码的签名
     * @param publicKeyStr Base64编码的公钥字符串
     * @return 验证结果
     */
    public static boolean verifySHA256(String data, String signature, String publicKeyStr) {
        try {
            RSAPublicKey publicKey = getPublicKey(publicKeyStr);
            return verifySHA256(data, signature, publicKey);
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] rsaSplitCodec(Cipher cipher, int opmode, byte[] datas, int keySize) {
        int maxBlock = 0;
        if (opmode == Cipher.DECRYPT_MODE) {
            maxBlock = keySize / 8;
        } else {
            maxBlock = keySize / 8 - 11;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;
        byte[] buff;
        int i = 0;
        try {
            while (datas.length > offSet) {
                if (datas.length - offSet > maxBlock) {
                    buff = cipher.doFinal(datas, offSet, maxBlock);
                } else {
                    buff = cipher.doFinal(datas, offSet, datas.length - offSet);
                }
                out.write(buff, 0, buff.length);
                i++;
                offSet = i * maxBlock;
            }
        } catch (Exception e) {
            throw new RuntimeException("加解密阀值为[" + maxBlock + "]的数据时发生异常", e);
        }
        byte[] result = out.toByteArray();
        IOUtils.closeQuietly(out);
        return result;
    }

}
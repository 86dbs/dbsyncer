/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.model;

/**
 * OpenAPI统一请求格式
 *
 * @author 穿云
 * @version 1.0.0
 */
public class OpenApiRequest {

    /**
     * 时间戳
     */
    private Long timestamp;

    /**
     * 随机数（防重放）
     */
    private String nonce;

    /**
     * 加密的数据（Base64编码）
     */
    private String encryptedData;

    /**
     * 初始化向量（Base64编码）
     */
    private String iv;

    /**
     * RSA加密的AES密钥（Base64编码）
     */
    private String encryptedKey;

    /**
     * 签名
     */
    private String signature;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getNonce() {
        return nonce;
    }

    public void setNonce(String nonce) {
        this.nonce = nonce;
    }

    public String getEncryptedData() {
        return encryptedData;
    }

    public void setEncryptedData(String encryptedData) {
        this.encryptedData = encryptedData;
    }

    public String getIv() {
        return iv;
    }

    public void setIv(String iv) {
        this.iv = iv;
    }

    public String getEncryptedKey() {
        return encryptedKey;
    }

    public void setEncryptedKey(String encryptedKey) {
        this.encryptedKey = encryptedKey;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }
}

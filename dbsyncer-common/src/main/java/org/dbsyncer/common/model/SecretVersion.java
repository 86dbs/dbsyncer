/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

/**
 * 密钥版本信息
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-02 00:04
 */
public class SecretVersion extends ConfigVersion {

    /**
     * 密钥
     */
    private String secret;

    /**
     * 密钥哈希值（SHA-1哈希后的Base64编码）
     */
    private String hashedSecret;

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getHashedSecret() {
        return hashedSecret;
    }

    public void setHashedSecret(String hashedSecret) {
        this.hashedSecret = hashedSecret;
    }

}

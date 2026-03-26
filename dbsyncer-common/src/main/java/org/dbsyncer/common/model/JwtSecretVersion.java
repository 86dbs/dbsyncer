/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

/**
 * JWT 密钥版本信息（历史密钥，用于验证旧 Token）
 *
 * @author 穿云
 * @version 2.0.0
 */
public class JwtSecretVersion extends ConfigVersion {

    /**
     * JWT 签名密钥（Base64 编码）
     */
    private String secret;

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }
}

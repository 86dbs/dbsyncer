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
public class SecretVersion {

    /**
     * 密钥
     */
    private String secret;

    /**
     * 密钥哈希值（SHA-1哈希后的Base64编码）
     */
    private String hashedSecret;

    /**
     * 版本号（从1开始递增）
     */
    private int version;

    /**
     * 创建时间（毫秒时间戳）
     */
    private Long createTime;

    /**
     * 是否启用
     */
    private boolean enabled = true;

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

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}

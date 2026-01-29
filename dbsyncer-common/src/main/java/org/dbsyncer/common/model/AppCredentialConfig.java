/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 业务系统凭证配置
 * 支持多版本密钥管理，用于平滑轮换
 *
 * @author 穿云
 * @version 2.0.0
 */
public class AppCredentialConfig {

    /**
     * 业务系统凭证Map（appId -> 密钥版本列表）
     * 每个业务系统可以有多个版本的密钥，支持平滑轮换
     */
    private Map<String, List<SecretVersion>> credentials;

    /**
     * 是否启用凭证验证
     */
    private boolean enabled = true;

    /**
     * 每个业务系统最大保留的密钥版本数量（默认5个）
     */
    private int maxVersionSize = 5;

    /**
     * 密钥版本信息
     */
    public static class SecretVersion {
        /**
         * 版本号（从1开始递增）
         */
        private int version;

        /**
         * 密钥哈希值（SHA-1哈希后的Base64编码）
         */
        private String hashedSecret;

        /**
         * 创建时间（毫秒时间戳）
         */
        private Long createTime;

        /**
         * 是否启用
         */
        private boolean enabled = true;

        public SecretVersion() {
        }

        public SecretVersion(int version, String hashedSecret, Long createTime) {
            this.version = version;
            this.hashedSecret = hashedSecret;
            this.createTime = createTime;
            this.enabled = true;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public String getHashedSecret() {
            return hashedSecret;
        }

        public void setHashedSecret(String hashedSecret) {
            this.hashedSecret = hashedSecret;
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

    public Map<String, List<SecretVersion>> getCredentials() {
        if (credentials == null) {
            credentials = new HashMap<>();
        }
        return credentials;
    }

    public void setCredentials(Map<String, List<SecretVersion>> credentials) {
        this.credentials = credentials;
    }

    /**
     * 获取业务系统的密钥版本列表
     * 
     * @param appId 业务系统标识
     * @return 密钥版本列表，如果不存在返回空列表
     */
    public List<SecretVersion> getSecretVersions(String appId) {
        if (credentials == null) {
            return new ArrayList<>();
        }
        List<SecretVersion> versions = credentials.get(appId);
        return versions != null ? versions : new ArrayList<>();
    }

    /**
     * 添加业务系统密钥版本
     * 
     * @param appId 业务系统标识
     * @param version 密钥版本
     */
    public void addSecretVersion(String appId, SecretVersion version) {
        if (credentials == null) {
            credentials = new HashMap<>();
        }
        List<SecretVersion> versions = credentials.computeIfAbsent(appId, k -> new ArrayList<>());
        versions.add(version);
    }

    /**
     * 设置业务系统的密钥版本列表
     * 
     * @param appId 业务系统标识
     * @param versions 密钥版本列表
     */
    public void setSecretVersions(String appId, List<SecretVersion> versions) {
        if (credentials == null) {
            credentials = new HashMap<>();
        }
        credentials.put(appId, versions);
    }

    /**
     * 移除业务系统凭证（移除所有版本）
     * 
     * @param appId 业务系统标识
     */
    public void removeCredential(String appId) {
        if (credentials != null) {
            credentials.remove(appId);
        }
    }

    /**
     * 检查业务系统是否存在
     * 
     * @param appId 业务系统标识
     * @return 是否存在
     */
    public boolean containsAppId(String appId) {
        return credentials != null && credentials.containsKey(appId) && !credentials.get(appId).isEmpty();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getMaxVersionSize() {
        return maxVersionSize;
    }

    public void setMaxVersionSize(int maxVersionSize) {
        this.maxVersionSize = maxVersionSize;
    }
}

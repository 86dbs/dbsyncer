/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import java.util.HashMap;
import java.util.Map;

/**
 * JWT密钥配置
 * 支持密钥版本管理和多个历史密钥，用于平滑轮换
 *
 * @author 穿云
 * @version 2.0.0
 */
public class JwtSecretConfig {

    /**
     * 当前密钥版本
     */
    private int currentVersion = 1;

    /**
     * 当前密钥（用于生成新Token）
     */
    private String currentSecret;

    /**
     * 历史密钥Map（版本号 -> 密钥）
     * 用于验证旧Token，支持多个历史密钥
     */
    private Map<Integer, String> historySecrets;

    /**
     * 密钥生成时间（毫秒时间戳）
     */
    private Long generateTime;

    /**
     * 密钥长度（默认256位，32字节）
     */
    private int secretLength = 32;

    /**
     * 最大保留的历史密钥数量（默认5个）
     */
    private int maxHistorySize = 5;

    public int getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(int currentVersion) {
        this.currentVersion = currentVersion;
    }

    public String getCurrentSecret() {
        return currentSecret;
    }

    public void setCurrentSecret(String currentSecret) {
        this.currentSecret = currentSecret;
    }

    public Map<Integer, String> getHistorySecrets() {
        if (historySecrets == null) {
            historySecrets = new HashMap<>();
        }
        return historySecrets;
    }

    public void setHistorySecrets(Map<Integer, String> historySecrets) {
        this.historySecrets = historySecrets;
    }

    /**
     * 添加历史密钥
     * 
     * @param version 版本号
     * @param secret 密钥
     */
    public void addHistorySecret(int version, String secret) {
        if (historySecrets == null) {
            historySecrets = new HashMap<>();
        }
        historySecrets.put(version, secret);
    }

    /**
     * 获取历史密钥
     * 
     * @param version 版本号
     * @return 密钥，如果不存在返回null
     */
    public String getHistorySecret(int version) {
        if (historySecrets == null) {
            return null;
        }
        return historySecrets.get(version);
    }

    public Long getGenerateTime() {
        return generateTime;
    }

    public void setGenerateTime(Long generateTime) {
        this.generateTime = generateTime;
    }

    public int getSecretLength() {
        return secretLength;
    }

    public void setSecretLength(int secretLength) {
        this.secretLength = secretLength;
    }

    public int getMaxHistorySize() {
        return maxHistorySize;
    }

    public void setMaxHistorySize(int maxHistorySize) {
        this.maxHistorySize = maxHistorySize;
    }
}

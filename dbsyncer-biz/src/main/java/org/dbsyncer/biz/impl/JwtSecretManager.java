/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.common.model.JwtSecretConfig;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * JWT密钥管理器
 * 负责JWT密钥的生成、存储、获取和轮换
 * 支持多个历史密钥，实现平滑轮换
 * 
 * @author 穿云
 * @version 2.0.0
 */
@Component
public class JwtSecretManager {

    private static final Logger logger = LoggerFactory.getLogger(JwtSecretManager.class);

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private ProfileComponent profileComponent;

    /**
     * 默认密钥长度（字节数）
     */
    private static final int DEFAULT_SECRET_LENGTH = 32;

    /**
     * 默认最大保留的历史密钥数量
     */
    private static final int DEFAULT_MAX_HISTORY_SIZE = 5;

    /**
     * 获取当前JWT密钥（用于生成新Token）
     * 如果密钥不存在，自动生成
     * 
     * @return JWT密钥
     */
    public String getCurrentSecret() {
        JwtSecretConfig config = getJwtSecretConfig();
        if (config == null || StringUtil.isBlank(config.getCurrentSecret())) {
            logger.warn("JWT密钥不存在，自动生成新密钥");
            generateAndSaveSecret();
            config = getJwtSecretConfig();
        }
        return config.getCurrentSecret();
    }

    /**
     * 获取用于验证Token的密钥
     * 优先使用当前密钥，如果验证失败，按版本号从高到低尝试历史密钥（支持平滑轮换）
     * 
     * @return JWT密钥数组，第一个是当前密钥，后面是历史密钥（按版本号从高到低排序）
     */
    public String[] getSecretsForVerification() {
        JwtSecretConfig config = getJwtSecretConfig();
        if (config == null || StringUtil.isBlank(config.getCurrentSecret())) {
            // 如果密钥不存在，生成新密钥
            generateAndSaveSecret();
            config = getJwtSecretConfig();
        }

        List<String> secrets = new ArrayList<>();
        // 首先添加当前密钥
        secrets.add(config.getCurrentSecret());

        // 添加历史密钥（按版本号从高到低排序）
        Map<Integer, String> historySecrets = config.getHistorySecrets();
        if (historySecrets != null && !historySecrets.isEmpty()) {
            List<Integer> versions = new ArrayList<>(historySecrets.keySet());
            Collections.sort(versions, Collections.reverseOrder()); // 从高到低排序
            
            for (Integer version : versions) {
                String secret = historySecrets.get(version);
                if (StringUtil.isNotBlank(secret)) {
                    secrets.add(secret);
                }
            }
        }

        return secrets.toArray(new String[0]);
    }

    /**
     * 生成新的JWT密钥并保存
     * 如果存在旧密钥，会将其添加到历史密钥Map中，实现平滑轮换
     * 会自动清理过旧的历史密钥，只保留最近N个版本
     * 
     * @return 新生成的密钥
     */
    public String generateAndSaveSecret() {
        try {
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (systemConfig == null) {
                throw new RuntimeException("系统配置不存在");
            }

            JwtSecretConfig jwtSecretConfig = getJwtSecretConfig();
            if (jwtSecretConfig == null) {
                jwtSecretConfig = new JwtSecretConfig();
                jwtSecretConfig.setMaxHistorySize(DEFAULT_MAX_HISTORY_SIZE);
            }

            // 保存当前密钥到历史密钥Map（如果存在）
            if (StringUtil.isNotBlank(jwtSecretConfig.getCurrentSecret())) {
                int oldVersion = jwtSecretConfig.getCurrentVersion();
                String oldSecret = jwtSecretConfig.getCurrentSecret();
                jwtSecretConfig.addHistorySecret(oldVersion, oldSecret);
                logger.info("保存当前密钥到历史密钥，版本: {}", oldVersion);
            }

            // 清理过旧的历史密钥，只保留最近N个版本
            cleanupOldHistorySecrets(jwtSecretConfig);

            // 生成新密钥
            String newSecret = generateSecret(DEFAULT_SECRET_LENGTH);
            int newVersion = jwtSecretConfig.getCurrentVersion() + 1;
            jwtSecretConfig.setCurrentSecret(newSecret);
            jwtSecretConfig.setCurrentVersion(newVersion);
            jwtSecretConfig.setGenerateTime(System.currentTimeMillis());

            // 保存到系统配置
            saveJwtSecretConfig(jwtSecretConfig);

            logger.info("生成新的JWT密钥成功，版本: {}，历史密钥数量: {}", 
                    newVersion, jwtSecretConfig.getHistorySecrets().size());
            return newSecret;
        } catch (Exception e) {
            logger.error("生成并保存JWT密钥失败", e);
            throw new RuntimeException("生成并保存JWT密钥失败", e);
        }
    }

    /**
     * 清理过旧的历史密钥，只保留最近N个版本
     * 
     * @param jwtSecretConfig JWT密钥配置
     */
    private void cleanupOldHistorySecrets(JwtSecretConfig jwtSecretConfig) {
        Map<Integer, String> historySecrets = jwtSecretConfig.getHistorySecrets();
        if (historySecrets == null || historySecrets.isEmpty()) {
            return;
        }

        int maxHistorySize = jwtSecretConfig.getMaxHistorySize();
        if (maxHistorySize <= 0) {
            maxHistorySize = DEFAULT_MAX_HISTORY_SIZE;
        }

        // 如果历史密钥数量超过限制，删除最旧的版本
        if (historySecrets.size() >= maxHistorySize) {
            List<Integer> versions = new ArrayList<>(historySecrets.keySet());
            Collections.sort(versions); // 从低到高排序
            
            // 删除最旧的版本，直到数量符合要求
            int removeCount = historySecrets.size() - maxHistorySize + 1; // +1 因为即将添加一个新版本
            for (int i = 0; i < removeCount && i < versions.size(); i++) {
                int oldVersion = versions.get(i);
                historySecrets.remove(oldVersion);
                logger.info("清理过旧的历史密钥，版本: {}", oldVersion);
            }
        }
    }

    /**
     * 轮换密钥
     * 生成新密钥，旧密钥保留用于验证旧Token
     * 
     * @return 新生成的密钥
     */
    public String rotateSecret() {
        logger.info("开始轮换JWT密钥");
        return generateAndSaveSecret();
    }

    /**
     * 生成随机密钥
     * 
     * @param length 密钥长度（字节数）
     * @return Base64编码的密钥
     */
    public static String generateSecret(int length) {
        try {
            SecureRandom secureRandom = new SecureRandom();
            byte[] secretBytes = new byte[length];
            secureRandom.nextBytes(secretBytes);
            return Base64.getEncoder().encodeToString(secretBytes);
        } catch (Exception e) {
            logger.error("生成随机密钥失败", e);
            throw new RuntimeException("生成随机密钥失败", e);
        }
    }

    /**
     * 从系统配置中获取JWT密钥配置
     * 
     * @return JWT密钥配置，如果不存在返回null
     */
    private JwtSecretConfig getJwtSecretConfig() {
        try {
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (systemConfig == null) {
                return null;
            }

            return systemConfig.getJwtSecretConfig();
        } catch (Exception e) {
            logger.error("获取JWT密钥配置失败", e);
            return null;
        }
    }

    /**
     * 保存JWT密钥配置到系统配置
     * 
     * @param jwtSecretConfig JWT密钥配置
     */
    private void saveJwtSecretConfig(JwtSecretConfig jwtSecretConfig) {
        try {
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (systemConfig == null) {
                throw new RuntimeException("系统配置不存在");
            }

            // 设置JWT密钥配置
            systemConfig.setJwtSecretConfig(jwtSecretConfig);

            // 将JWT密钥配置序列化为JSON字符串，保存到系统配置的扩展字段中
            // 由于SystemConfig是通过JSON序列化的，JwtSecretConfig会自动序列化
            // 但为了确保兼容性，我们通过ProfileComponent直接更新配置
            profileComponent.editConfigModel(systemConfig);

            logger.info("保存JWT密钥配置成功，版本: {}", jwtSecretConfig.getCurrentVersion());
        } catch (Exception e) {
            logger.error("保存JWT密钥配置失败", e);
            throw new RuntimeException("保存JWT密钥配置失败", e);
        }
    }
}

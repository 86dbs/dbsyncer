/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.common.model.AppCredentialConfig;
import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 业务系统凭证管理器
 * 负责业务系统 appId 和 appSecret 的验证和管理
 * 支持多版本密钥，实现平滑轮换
 * 
 * @author 穿云
 * @version 2.0.0
 */
@Component
public class AppCredentialManager {

    private static final Logger logger = LoggerFactory.getLogger(AppCredentialManager.class);

    /**
     * 默认最大保留的密钥版本数量
     */
    private static final int DEFAULT_MAX_VERSION_SIZE = 5;

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private ProfileComponent profileComponent;

    /**
     * 验证业务系统凭证
     * 支持多版本密钥，按版本号从高到低尝试验证
     * 
     * @param appId 业务系统标识
     * @param appSecret 业务系统密钥（原始密钥，会进行哈希后比较）
     * @return 验证是否通过
     */
    public boolean validateCredential(String appId, String appSecret) {
        try {
            if (StringUtil.isBlank(appId) || StringUtil.isBlank(appSecret)) {
                logger.warn("appId或appSecret为空");
                return false;
            }

            AppCredentialConfig config = getAppCredentialConfig();
            if (config == null || !config.isEnabled()) {
                logger.warn("业务系统凭证配置未启用或不存在");
                return false;
            }

            // 获取业务系统的所有密钥版本
            List<AppCredentialConfig.SecretVersion> versions = config.getSecretVersions(appId);
            if (versions == null || versions.isEmpty()) {
                logger.warn("业务系统 {} 不存在", appId);
                return false;
            }

            // 对输入的密钥进行哈希处理
            String hashedSecret = SHA1Util.b64_sha1(appSecret);

            // 按版本号从高到低排序，优先尝试最新版本
            List<AppCredentialConfig.SecretVersion> sortedVersions = new ArrayList<>(versions);
            sortedVersions.sort(Comparator.comparingInt(AppCredentialConfig.SecretVersion::getVersion).reversed());

            // 尝试所有启用的密钥版本
            for (AppCredentialConfig.SecretVersion version : sortedVersions) {
                if (!version.isEnabled()) {
                    continue;
                }

                if (StringUtil.equals(version.getHashedSecret(), hashedSecret)) {
                    logger.info("业务系统 {} 凭证验证成功，使用版本: {}", appId, version.getVersion());
                    return true;
                }
            }

            logger.warn("业务系统 {} 凭证验证失败，所有版本都不匹配", appId);
            return false;
        } catch (Exception e) {
            logger.error("验证业务系统凭证失败", e);
            return false;
        }
    }

    /**
     * 添加业务系统凭证
     * 如果业务系统已存在，会创建新版本密钥，保留旧版本用于验证
     * 
     * @param appId 业务系统标识
     * @param appSecret 业务系统密钥（原始密钥，会自动进行哈希存储）
     * @return 是否添加成功
     */
    public boolean addCredential(String appId, String appSecret) {
        try {
            if (StringUtil.isBlank(appId) || StringUtil.isBlank(appSecret)) {
                logger.warn("appId或appSecret为空");
                return false;
            }

            AppCredentialConfig config = getAppCredentialConfig();
            if (config == null) {
                config = new AppCredentialConfig();
                config.setEnabled(true);
                config.setMaxVersionSize(DEFAULT_MAX_VERSION_SIZE);
            }

            // 获取现有密钥版本列表
            List<AppCredentialConfig.SecretVersion> versions = config.getSecretVersions(appId);
            
            // 计算新版本号
            int newVersion = 1;
            if (!versions.isEmpty()) {
                int maxVersion = versions.stream()
                        .mapToInt(AppCredentialConfig.SecretVersion::getVersion)
                        .max()
                        .orElse(0);
                newVersion = maxVersion + 1;
            }

            // 对密钥进行哈希处理
            String hashedSecret = SHA1Util.b64_sha1(appSecret);

            // 创建新版本
            AppCredentialConfig.SecretVersion newVersionObj = new AppCredentialConfig.SecretVersion(
                    newVersion, hashedSecret, System.currentTimeMillis());

            // 添加到版本列表
            config.addSecretVersion(appId, newVersionObj);

            // 清理过旧的版本
            cleanupOldVersions(config, appId);

            // 保存配置
            saveAppCredentialConfig(config);

            logger.info("添加业务系统凭证成功，appId: {}，版本: {}", appId, newVersion);
            return true;
        } catch (Exception e) {
            logger.error("添加业务系统凭证失败", e);
            return false;
        }
    }

    /**
     * 轮换业务系统密钥
     * 生成新版本密钥，保留旧版本用于验证
     * 
     * @param appId 业务系统标识
     * @param newAppSecret 新的业务系统密钥
     * @return 是否轮换成功
     */
    public boolean rotateCredential(String appId, String newAppSecret) {
        logger.info("开始轮换业务系统 {} 的密钥", appId);
        return addCredential(appId, newAppSecret);
    }

    /**
     * 移除业务系统凭证
     * 
     * @param appId 业务系统标识
     * @return 是否移除成功
     */
    public boolean removeCredential(String appId) {
        try {
            if (StringUtil.isBlank(appId)) {
                logger.warn("appId为空");
                return false;
            }

            AppCredentialConfig config = getAppCredentialConfig();
            if (config == null) {
                logger.warn("业务系统凭证配置不存在");
                return false;
            }

            config.removeCredential(appId);
            saveAppCredentialConfig(config);

            logger.info("移除业务系统凭证成功，appId: {}", appId);
            return true;
        } catch (Exception e) {
            logger.error("移除业务系统凭证失败", e);
            return false;
        }
    }

    /**
     * 检查业务系统是否存在
     * 
     * @param appId 业务系统标识
     * @return 是否存在
     */
    public boolean containsAppId(String appId) {
        AppCredentialConfig config = getAppCredentialConfig();
        return config != null && config.containsAppId(appId);
    }

    /**
     * 获取业务系统的密钥版本数量
     * 
     * @param appId 业务系统标识
     * @return 密钥版本数量
     */
    public int getVersionCount(String appId) {
        AppCredentialConfig config = getAppCredentialConfig();
        if (config == null) {
            return 0;
        }
        return config.getSecretVersions(appId).size();
    }

    /**
     * 清理过旧的密钥版本，只保留最近N个版本
     * 
     * @param config 凭证配置
     * @param appId 业务系统标识
     */
    private void cleanupOldVersions(AppCredentialConfig config, String appId) {
        List<AppCredentialConfig.SecretVersion> versions = config.getSecretVersions(appId);
        if (versions == null || versions.isEmpty()) {
            return;
        }

        int maxVersionSize = config.getMaxVersionSize();
        if (maxVersionSize <= 0) {
            maxVersionSize = DEFAULT_MAX_VERSION_SIZE;
        }

        // 如果版本数量超过限制，删除最旧的版本
        if (versions.size() > maxVersionSize) {
            // 按版本号从低到高排序
            versions.sort(Comparator.comparingInt(AppCredentialConfig.SecretVersion::getVersion));

            // 删除最旧的版本
            int removeCount = versions.size() - maxVersionSize;
            for (int i = 0; i < removeCount; i++) {
                AppCredentialConfig.SecretVersion oldVersion = versions.remove(0);
                logger.info("清理过旧的密钥版本，appId: {}，版本: {}", appId, oldVersion.getVersion());
            }
        }
    }

    /**
     * 从系统配置中获取业务系统凭证配置
     * 
     * @return 业务系统凭证配置，如果不存在返回null
     */
    private AppCredentialConfig getAppCredentialConfig() {
        try {
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (systemConfig == null) {
                return null;
            }

            return systemConfig.getAppCredentialConfig();
        } catch (Exception e) {
            logger.error("获取业务系统凭证配置失败", e);
            return null;
        }
    }

    /**
     * 保存业务系统凭证配置到系统配置
     * 
     * @param appCredentialConfig 业务系统凭证配置
     */
    private void saveAppCredentialConfig(AppCredentialConfig appCredentialConfig) {
        try {
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (systemConfig == null) {
                throw new RuntimeException("系统配置不存在");
            }

            // 设置业务系统凭证配置
            systemConfig.setAppCredentialConfig(appCredentialConfig);

            // 保存到系统配置
            profileComponent.editConfigModel(systemConfig);

            logger.info("保存业务系统凭证配置成功");
        } catch (Exception e) {
            logger.error("保存业务系统凭证配置失败", e);
            throw new RuntimeException("保存业务系统凭证配置失败", e);
        }
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.common.model.CredentialConfig;
import org.dbsyncer.common.model.SecretVersion;
import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 业务系统凭证管理器
 * 支持多版本密钥，实现平滑轮换
 *
 * @author 穿云
 * @version 2.0.0
 */
@Component
public class CredentialManager {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 默认最大保留的密钥版本数量
     */
    private static final int DEFAULT_MAX_VERSION_SIZE = 5;

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private ProfileComponent profileComponent;

    /**
     * 验证系统凭证
     * 支持多版本密钥，按版本号从高到低尝试验证
     *
     * @param secret 系统密钥（原始密钥，会进行哈希后比较）
     * @return 验证是否通过
     */
    public boolean validateCredential(String secret) {
        Assert.hasText(secret, "secret为空");
        CredentialConfig config = getCredentialConfig();
        Assert.notNull(config, "系统凭证配置未启用");
        Assert.notEmpty(config.getSecretVersions(), "系统凭证配置未启用");

        // 对输入的密钥进行哈希处理
        String hashedSecret = SHA1Util.b64_sha1(secret);

        // 按版本号从高到低排序，优先尝试最新版本
        List<SecretVersion> sortedVersions = config.getSecretVersions().stream().sorted(Comparator.comparingInt(SecretVersion::getVersion).reversed()).collect(Collectors.toList());

        // 尝试所有启用的密钥版本
        for (SecretVersion version : sortedVersions) {
            if (!version.isEnabled()) {
                continue;
            }
            if (StringUtil.equals(version.getHashedSecret(), hashedSecret)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 添加系统凭证
     * 如果已存在，会创建新版本密钥，保留旧版本用于验证
     *
     * @param secret 系统密钥（原始密钥，会自动进行哈希存储）
     * @return 是否添加成功
     */
    public boolean addCredential(String secret) {
        Assert.hasText(secret, "secret为空");

        CredentialConfig config = getCredentialConfig();
        if (config == null) {
            config = new CredentialConfig();
            config.setMaxVersionSize(DEFAULT_MAX_VERSION_SIZE);
        }

        // 获取现有密钥版本列表
        List<SecretVersion> versions = config.getSecretVersions();

        // 计算新版本号
        int newVersion = 1;
        if (!versions.isEmpty()) {
            int maxVersion = versions.stream().mapToInt(SecretVersion::getVersion).max().orElse(0);
            newVersion = maxVersion + 1;
        }

        // 对密钥进行哈希处理
        String hashedSecret = SHA1Util.b64_sha1(secret);

        // 创建新版本
        SecretVersion newVersionObj = new SecretVersion();
        newVersionObj.setSecret(secret);
        newVersionObj.setHashedSecret(hashedSecret);
        newVersionObj.setVersion(newVersion);
        newVersionObj.setCreateTime(Instant.now().toEpochMilli());
        newVersionObj.setEnabled(true);
        // 添加到版本列表
        versions.add(newVersionObj);

        // 清理过旧的版本
        cleanupOldVersions(versions);

        // 保存配置
        saveCredentialConfig(config);
        logger.info("添加业务系统凭证成功，secret: {}，版本: {}", hashedSecret, newVersion);
        return true;
    }

    /**
     * 移除系统凭证
     *
     * @param secret 系统密钥
     */
    public void removeCredential(String secret) {
        Assert.hasText(secret, "secret为空");
        CredentialConfig config = getCredentialConfig();
        if (config == null) {
            return;
        }

        Iterator<SecretVersion> iterator = config.getSecretVersions().iterator();
        while (iterator.hasNext()) {
            SecretVersion version = iterator.next();
            if (version.getSecret().equals(secret)) {
                iterator.remove();
                saveCredentialConfig(config);
                logger.info("移除系统凭证成功，secret:{}, version:{}", version.getHashedSecret(), version.getVersion());
                break;
            }
        }
    }

    /**
     * 清理过旧的密钥版本，只保留最近N个版本
     *
     * @param versions 凭证配置列表
     */
    private void cleanupOldVersions(List<SecretVersion> versions) {
        // 如果版本数量超过限制，删除最旧的版本
        if (versions.size() > DEFAULT_MAX_VERSION_SIZE) {
            // 按版本号从低到高排序
            versions.sort(Comparator.comparingInt(SecretVersion::getVersion));
            // 删除最旧的版本
            SecretVersion remove = versions.remove(0);
            logger.info("清理过旧的密钥，secret: {}，版本: {}", remove.getSecret(), remove.getVersion());
        }
    }

    /**
     * 从系统配置中获取系统凭证配置
     *
     * @return 系统凭证配置，如果不存在返回null
     */
    private CredentialConfig getCredentialConfig() {
        try {
            return systemConfigService.getSystemConfig().getCredentialConfig();
        } catch (Exception e) {
            logger.error("获取系统凭证配置失败", e);
            return null;
        }
    }

    /**
     * 保存系统凭证配置到系统配置
     *
     * @param credentialConfig 系统凭证配置
     */
    private void saveCredentialConfig(CredentialConfig credentialConfig) {
        SystemConfig systemConfig = systemConfigService.getSystemConfig();
        if (systemConfig == null) {
            throw new RuntimeException("系统配置不存在");
        }

        // 设置业务系统凭证配置
        systemConfig.setCredentialConfig(credentialConfig);

        // 保存到系统配置
        profileComponent.editConfigModel(systemConfig);
        logger.info("保存系统凭证配置成功");
    }
}

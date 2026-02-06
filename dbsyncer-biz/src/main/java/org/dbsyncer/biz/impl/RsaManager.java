/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.common.model.RsaConfig;
import org.dbsyncer.common.model.RsaVersion;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.CryptoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * RSA管理器
 *
 * @author 穿云
 * @version 2.0.0
 * @see RsaManager
 */
@Component
public class RsaManager {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 默认最大保留的密钥版本数量
     */
    private static final int DEFAULT_MAX_VERSION_SIZE = 3;

    /**
     * 解密请求参数
     * 支持多版本密钥，按版本号从高到低尝试验证
     *
     * @param config 当前配置
     * @param requestBody 请求参数
     * @param isPublicNetwork 是否公网请求
     * @return 验证是否通过
     */
    public String decryptedData(RsaConfig config, String requestBody, boolean isPublicNetwork) {
        Assert.hasText(requestBody, "requestBody为空");
        if (config == null || CollectionUtils.isEmpty(config.getRsaVersions())) {
            throw new BizException("RSA密钥配置未启用");
        }

        // 按版本号从高到低排序，优先尝试最新版本
        List<RsaVersion> sortedVersions = config.getRsaVersions().stream().sorted(Comparator.comparingInt(RsaVersion::getVersion).reversed()).collect(Collectors.toList());

        // 尝试所有启用的密钥版本
        for (RsaVersion version : sortedVersions) {
            if (!version.isEnabled()) {
                continue;
            }
            try {
                // 解析加密请求（解密数据）
                // TODO 待补充
                return CryptoUtil.parseEncryptedRequest(
                        requestBody,
                        version.getPrivateKey(),
                        version.getPublicKey(),
                        "",
                        isPublicNetwork,
                        String.class
                );
            } catch (Exception e) {
                logger.error("解密失败，版本: {}", version.getVersion());
            }
        }
        throw new BizException("解密失败，密钥验证均不通过");
    }

    /**
     * 添加API密钥
     * 如果已存在，会创建新版本密钥，保留旧版本用于验证
     *
     * @param config 当前配置
     * @param publicKey 公钥
     * @param privateKey 私钥
     * @param keyLength 密钥长度
     * @return 新的配置
     */
    public RsaConfig addCredential(RsaConfig config, String publicKey, String privateKey, int keyLength) {
        if (config == null) {
            config = new RsaConfig();
        }

        // 获取现有密钥版本列表
        List<RsaVersion> versions = config.getRsaVersions();

        // 计算新版本号
        int newVersion = 1;
        if (!versions.isEmpty()) {
            int maxVersion = versions.stream().mapToInt(RsaVersion::getVersion).max().orElse(0);
            newVersion = maxVersion + 1;
        }

        // 创建新版本
        RsaVersion newVersionObj = new RsaVersion();
        newVersionObj.setPublicKey(publicKey);
        newVersionObj.setPrivateKey(privateKey);
        newVersionObj.setKeyLength(keyLength);
        newVersionObj.setVersion(newVersion);
        newVersionObj.setCreateTime(Instant.now().toEpochMilli());
        newVersionObj.setEnabled(true);
        // 添加到版本列表
        versions.add(newVersionObj);

        // 清理过旧的版本
        cleanupOldVersions(versions);

        // 保存配置
        logger.info("添加RSA密钥成功，版本: {}", newVersion);
        return config;
    }

    /**
     * 清理过旧的密钥版本，只保留最近N个版本
     *
     * @param versions 凭证配置列表
     */
    private void cleanupOldVersions(List<RsaVersion> versions) {
        // 如果版本数量超过限制，删除最旧的版本
        if (versions.size() > DEFAULT_MAX_VERSION_SIZE) {
            // 按版本号从低到高排序
            versions.sort(Comparator.comparingInt(RsaVersion::getVersion));
            // 删除最旧的版本
            RsaVersion remove = versions.remove(0);
            logger.info("清理过旧的RSA密钥，publicKey: {}，版本: {}", remove.getPublicKey(), remove.getVersion());
        }
    }

}

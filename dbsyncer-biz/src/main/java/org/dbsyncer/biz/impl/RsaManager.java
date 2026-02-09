/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import com.alibaba.fastjson2.JSONObject;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.common.model.RsaConfig;
import org.dbsyncer.common.model.RsaVersion;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.CryptoUtil;
import org.dbsyncer.common.util.RSAUtil;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.security.interfaces.RSAPrivateKey;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

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
     * @param config          当前配置
     * @param data            加密数据
     * @param isPublicNetwork 是否公网请求
     * @return 验证是否通过
     */
    public String decryptData(RsaConfig config, JSONObject data, boolean isPublicNetwork) {
        if (config == null || CollectionUtils.isEmpty(config.getRsaVersions())) {
            throw new BizException("RSA密钥配置未启用");
        }

        // 尝试所有启用的密钥版本
        for (int i = config.getRsaVersions().size() - 1; i >= 0; i--) {
            RsaVersion version = config.getRsaVersions().get(i);
            if (!version.isEnabled()) {
                continue;
            }
            try {
                // 内网：未单独配置 hmacSecret 时，使用当前 RSA 公钥作为 HMAC 密钥（与客户端约定一致即可验签），性能方面影响可忽略不计。
                // 公网：不使用 HMAC，传空即可。
                String hmacSecret = isPublicNetwork ? StringUtil.EMPTY : version.getPublicKey();
                RSAPrivateKey privateKey = RSAUtil.getPrivateKey(version.getPrivateKey());
                return CryptoUtil.parseEncryptedRequest(data, privateKey, hmacSecret, isPublicNetwork);
            } catch (Exception e) {
                logger.error("解密失败，版本: {}", version.getVersion());
            }
        }
        throw new BizException("解密失败，密钥验证均不通过");
    }

    /**
     * 加密数据
     *
     * @param config          当前配置
     * @param data            返回数据
     * @param isPublicNetwork 是否公网请求
     */
    public Object encryptData(RsaConfig config, Object data, boolean isPublicNetwork) {
        if (config == null || data == null || CollectionUtils.isEmpty(config.getRsaVersions())) {
            return data;
        }

        // 尝试所有启用的密钥版本
        for (int i = config.getRsaVersions().size() - 1; i >= 0; i--) {
            RsaVersion version = config.getRsaVersions().get(i);
            if (!version.isEnabled()) {
                continue;
            }
            try {
                String hmacSecret = isPublicNetwork ? StringUtil.EMPTY : version.getPublicKey();
                RSAPrivateKey privateKey = RSAUtil.getPrivateKey(version.getPrivateKey());
                return CryptoUtil.buildEncryptedRequest(data, privateKey, hmacSecret, isPublicNetwork);
            } catch (Exception e) {
                logger.error("加密失败，版本: {}", version.getVersion());
            }
        }
        throw new BizException("加密失败");
    }

    /**
     * 添加API密钥
     * 如果已存在，会创建新版本密钥，保留旧版本用于验证
     *
     * @param config     当前配置
     * @param publicKey  公钥
     * @param privateKey 私钥
     * @param keyLength  密钥长度
     * @return 新的配置
     */
    public synchronized RsaConfig addCredential(RsaConfig config, String publicKey, String privateKey, int keyLength) {
        if (config == null) {
            config = new RsaConfig();
        }

        // 获取现有密钥版本列表
        List<RsaVersion> versions = config.getRsaVersions();

        // 已存在
        Optional<RsaVersion> exist = versions.stream().filter(v -> StringUtil.equals(v.getPublicKey(), publicKey) && StringUtil.equals(v.getPrivateKey(), privateKey)).findFirst();
        if (exist.isPresent()) {
            return config;
        }

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

        logger.info("添加RSA密钥成功，版本: {}", newVersion);

        // 清理过旧的版本
        cleanupOldVersions(versions);
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

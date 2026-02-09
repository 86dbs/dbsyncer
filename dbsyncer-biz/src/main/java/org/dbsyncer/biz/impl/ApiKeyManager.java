/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.common.model.ApiKeyConfig;
import org.dbsyncer.common.model.SecretVersion;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.common.util.StringUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * API密钥管理器（客户端凭证管理）
 * <p>
 * 负责管理外部系统接入的API密钥（API Key），用于OpenAPI的身份认证。
 * 支持多版本密钥，实现平滑轮换。
 * </p>
 *
 * <h3>与 JwtSecretManager 的区别：</h3>
 * <table border="1">
 *   <tr><th>维度</th><th>ApiKeyManager</th><th>JwtSecretManager</th></tr>
 *   <tr><td>职责</td><td>客户端身份认证（Who are you?）</td><td>会话令牌管理（授权访问）</td></tr>
 *   <tr><td>密钥持有方</td><td>客户端持有</td><td>服务端内部</td></tr>
 *   <tr><td>使用场景</td><td>登录时验证</td><td>生成和验证JWT Token</td></tr>
 *   <tr><td>密钥格式</td><td>SHA1哈希存储（不可逆）</td><td>Base64编码（用于签名）</td></tr>
 *   <tr><td>生命周期</td><td>长期有效</td><td>随Token过期轮换</td></tr>
 * </table>
 *
 * <h3>认证流程：</h3>
 * <pre>
 * 1. 客户端提交 API Key（secret）
 * 2. ApiKeyManager.validateCredential() 验证身份
 * 3. 验证通过后，JwtSecretManager 生成 JWT Token
 * 4. 后续请求携带 JWT Token，由 JwtSecretManager 验证
 * </pre>
 *
 * @author 穿云
 * @version 2.0.0
 * @see JwtSecretManager
 */
@Component
public class ApiKeyManager {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 默认最大保留的密钥版本数量
     */
    private static final int DEFAULT_MAX_VERSION_SIZE = 3;

    /**
     * 验证API密钥
     * 支持多版本密钥，按版本号从高到低尝试验证
     *
     * @param config 当前配置
     * @param secret API密钥（原始密钥，会进行哈希后比较）
     * @return 验证是否通过
     */
    public boolean validate(ApiKeyConfig config, String secret) {
        Assert.hasText(secret, "secret为空");
        if (config == null || CollectionUtils.isEmpty(config.getSecrets())) {
            logger.warn("API密钥配置未启用");
            return false;
        }

        // 对输入的密钥进行哈希处理
        String hashedSecret = SHA1Util.b64_sha1(secret);

        // 按版本号从高到低排序，优先尝试最新版本
        for (int i = config.getSecrets().size() - 1; i >= 0; i--) {
            SecretVersion version = config.getSecrets().get(i);
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
     * 添加API密钥
     * 如果已存在，会创建新版本密钥，保留旧版本用于验证
     *
     * @param config 当前配置
     * @param secret API密钥（原始密钥，会自动进行哈希存储）
     */
    public synchronized ApiKeyConfig addCredential(ApiKeyConfig config, String secret) {
        Assert.hasText(secret, "secret为空");

        if (config == null) {
            config = new ApiKeyConfig();
        }

        // 获取现有密钥版本列表
        List<SecretVersion> versions = config.getSecrets();

        // 对密钥进行哈希处理
        String hashedSecret = SHA1Util.b64_sha1(secret);

        // 已存在
        Optional<SecretVersion> exist = versions.stream().filter(v->StringUtil.equals(v.getHashedSecret(), hashedSecret)).findFirst();
        if (exist.isPresent()) {
            return config;
        }

        // 计算新版本号
        int newVersion = 1;
        if (!versions.isEmpty()) {
            int maxVersion = versions.stream().mapToInt(SecretVersion::getVersion).max().orElse(0);
            newVersion = maxVersion + 1;
        }
        // 创建新版本
        SecretVersion newVersionObj = new SecretVersion();
        newVersionObj.setSecret(secret);
        newVersionObj.setHashedSecret(hashedSecret);
        newVersionObj.setVersion(newVersion);
        newVersionObj.setCreateTime(Instant.now().toEpochMilli());
        newVersionObj.setEnabled(true);
        // 添加到版本列表
        versions.add(newVersionObj);

        logger.info("添加API密钥成功，版本: {}", newVersion);
        // 清理过旧的版本
        cleanupOldVersions(versions);

        return config;
    }

    /**
     * 清理过旧的密钥版本，只保留最近N个版本
     *
     * @param versions 凭证配置列表
     */
    private void cleanupOldVersions(List<SecretVersion> versions) {
        // 如果版本数量超过限制，删除最旧的版本
        if (versions.size() > DEFAULT_MAX_VERSION_SIZE) {
            // 删除最旧的版本
            SecretVersion remove = versions.remove(0);
            logger.info("清理过旧的API密钥，secret: {}，版本: {}", remove.getSecret(), remove.getVersion());
        }
    }

}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.model.TokenInfo;
import org.dbsyncer.biz.util.JwtUtil;
import org.dbsyncer.common.model.JwtSecretConfig;
import org.dbsyncer.common.model.JwtSecretVersion;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

/**
 * JWT密钥管理器（服务端签名密钥管理）
 * <p>
 * 负责JWT密钥的生成、存储、获取和轮换。
 * 支持多个历史密钥，实现平滑轮换。
 * </p>
 *
 * <h3>与 ApiKeyManager 的区别：</h3>
 * <table border="1">
 *   <tr><th>维度</th><th>JwtSecretManager</th><th>ApiKeyManager</th></tr>
 *   <tr><td>职责</td><td>会话令牌管理（授权访问）</td><td>客户端身份认证（Who are you?）</td></tr>
 *   <tr><td>密钥持有方</td><td>服务端内部</td><td>客户端持有</td></tr>
 *   <tr><td>使用场景</td><td>生成和验证JWT Token</td><td>登录时验证</td></tr>
 *   <tr><td>密钥格式</td><td>Base64编码（用于签名）</td><td>SHA1哈希存储（不可逆）</td></tr>
 *   <tr><td>生命周期</td><td>随Token过期轮换</td><td>长期有效</td></tr>
 * </table>
 *
 * <h3>认证流程：</h3>
 * <pre>
 * 1. 客户端提交 API Key（secret）
 * 2. ApiKeyManager.validate() 验证身份
 * 3. 验证通过后，JwtSecretManager 生成 JWT Token
 * 4. 后续请求携带 JWT Token，由 JwtSecretManager 验证
 * </pre>
 *
 * @author 穿云
 * @version 2.0.0
 * @see ApiKeyManager
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
    private static final int DEFAULT_MAX_VERSION_SIZE = 3;

    /**
     * 获取当前JWT密钥（用于生成新Token）
     * 如果密钥不存在，自动生成
     * 
     * @return JWT密钥
     */
    public String generateToken() throws NoSuchAlgorithmException, InvalidKeyException {
        JwtSecretConfig config = getJwtSecretConfig();
        List<JwtSecretVersion> secrets = config.getSecrets();
        String secret = CollectionUtils.isEmpty(secrets) ? StringUtil.EMPTY : secrets.get(secrets.size() - 1).getSecret();
        return JwtUtil.generateToken(secret);
    }

    /**
     * 获取JWT密钥（支持密钥轮换）
     *
     * @param token 当前使用的token
     * @return 生成新的JWT密钥
     */
    public String refreshToken(String token) throws NoSuchAlgorithmException, InvalidKeyException {
        JwtSecretConfig config = getJwtSecretConfig();
        // 从最新的密钥开始验证
        for (int i = config.getSecrets().size() - 1; i >= 0; i--) {
            JwtSecretVersion version = config.getSecrets().get(i);
            String newToken = JwtUtil.refreshToken(token, version.getSecret());
            if (newToken != null) {
                return newToken;
            }
        }
        return null;
    }

    /**
     * 校验JWT密钥（支持密钥轮换）
     *
     * @param token 当前使用的token
     * @return 校验结果true/false
     */
    public boolean verifyToken(String token) throws NoSuchAlgorithmException, InvalidKeyException {
        if (StringUtil.isBlank(token)) {
            return false;
        }
        JwtSecretConfig config = getJwtSecretConfig();
        // 从最新的密钥开始验证
        for (int i = config.getSecrets().size() - 1; i >= 0; i--) {
            JwtSecretVersion version = config.getSecrets().get(i);
            TokenInfo tokenInfo = JwtUtil.verifyToken(token, version.getSecret());
            if (tokenInfo != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * 从系统配置中获取JWT密钥配置
     *
     * @return JWT密钥配置，如果不存在返回null
     */
    private JwtSecretConfig getJwtSecretConfig() {
        SystemConfig systemConfig = systemConfigService.getSystemConfig();
        Assert.notNull(systemConfig,"系统服务暂不可用，请重试");
        JwtSecretConfig config = systemConfig.getJwtSecretConfig();
        if (config == null) {
            generateAndSaveSecret(systemConfig);
            config = systemConfig.getJwtSecretConfig();
        }
        return config;
    }

    /**
     * 生成新的JWT密钥并保存
     * 如果存在旧密钥，会将其添加到历史密钥Map中，实现平滑轮换
     * 会自动清理过旧的历史密钥，只保留最近N个版本
     */
    private synchronized void generateAndSaveSecret(SystemConfig systemConfig) {
        try {
            JwtSecretConfig config = systemConfig.getJwtSecretConfig();
            if (config == null) {
                config = new JwtSecretConfig();
            }
            List<JwtSecretVersion> versions = config.getSecrets();

            // 计算新版本号
            int newVersion = 1;
            if (!versions.isEmpty()) {
                int maxVersion = versions.stream().mapToInt(JwtSecretVersion::getVersion).max().orElse(0);
                newVersion = maxVersion + 1;
            }

            // 生成新密钥
            JwtSecretVersion newVersionObj = new JwtSecretVersion();
            newVersionObj.setSecret(generateSecret());
            newVersionObj.setVersion(newVersion);
            newVersionObj.setCreateTime(Instant.now().toEpochMilli());
            newVersionObj.setEnabled(true);
            versions.add(newVersionObj);

            // 清理过旧的历史密钥，只保留最近N个版本
            cleanupOldVersions(versions);

            // 保存到系统配置
            systemConfig.setJwtSecretConfig(config);
            profileComponent.editConfigModel(systemConfig);
            logger.info("生成新的JWT密钥成功，版本: {}，历史密钥数量: {}", newVersion, versions.size());
        } catch (Exception e) {
            logger.error("生成并保存JWT密钥失败", e);
            throw new BizException("生成并保存JWT密钥失败", e);
        }
    }

    /**
     * 清理过旧的密钥版本，只保留最近N个版本
     *
     * @param versions 凭证配置列表
     */
    private void cleanupOldVersions(List<JwtSecretVersion> versions) {
        // 如果版本数量超过限制，删除最旧的版本
        if (versions.size() > DEFAULT_MAX_VERSION_SIZE) {
            // 删除最旧的版本
            JwtSecretVersion remove = versions.remove(0);
            logger.info("清理过旧的JWT密钥，secret: {}，版本: {}", remove.getSecret(), remove.getVersion());
        }
    }

    /**
     * 生成随机密钥
     *
     * @return Base64编码的密钥
     */
    private String generateSecret() {
        try {
            SecureRandom secureRandom = new SecureRandom();
            byte[] secretBytes = new byte[DEFAULT_SECRET_LENGTH];
            secureRandom.nextBytes(secretBytes);
            return Base64.getEncoder().encodeToString(secretBytes);
        } catch (Exception e) {
            logger.error("生成随机密钥失败", e);
            throw new RuntimeException("生成随机密钥失败", e);
        }
    }

}

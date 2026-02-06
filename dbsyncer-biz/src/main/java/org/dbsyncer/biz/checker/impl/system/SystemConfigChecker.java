/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.system;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.impl.ApiKeyManager;
import org.dbsyncer.biz.impl.RsaManager;
import org.dbsyncer.common.model.ApiKeyConfig;
import org.dbsyncer.common.model.IpWhitelistConfig;
import org.dbsyncer.common.model.RsaConfig;
import org.dbsyncer.common.util.BeanUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class SystemConfigChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Resource
    private ApiKeyManager apiKeyManager;

    @Resource
    private RsaManager rsaManager;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        SystemConfig systemConfig = new SystemConfig();
        systemConfig.setName("系统配置");

        // 修改基本配置
        this.modifyConfigModel(systemConfig, params);

        profileComponent.addConfigModel(systemConfig);
        return systemConfig;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        printParams(params);
        Assert.notEmpty(params, "Config check params is null.");
        Map<String, Object> newParams = new HashMap<>();
        newParams.putAll(params);
        newParams.put("enableStorageWriteSuccess", StringUtil.isNotBlank(params.get("enableStorageWriteSuccess")));
        newParams.put("enableStorageWriteFail", StringUtil.isNotBlank(params.get("enableStorageWriteFail")));
        newParams.put("enableStorageWriteFull", StringUtil.isNotBlank(params.get("enableStorageWriteFull")));
        newParams.put("enableWatermark", StringUtil.isNotBlank(params.get("enableWatermark")));
        newParams.put("enablePrintTraceInfo", StringUtil.isNotBlank(params.get("enablePrintTraceInfo")));
        newParams.put("enableOpenAPI", StringUtil.isNotBlank(params.get("enableOpenAPI")));
        String watermark = params.get("watermark");
        if (StringUtil.isNotBlank(watermark)) {
            Assert.isTrue(watermark.length() <= 64, "允许水印内容最多输入64个字.");
        }
        params.put("watermark", watermark);

        SystemConfig systemConfig = profileComponent.getSystemConfig();
        Assert.notNull(systemConfig, "配置文件为空.");
        BeanUtil.mapToBean(newParams, systemConfig);
        // 修改 API 密钥配置
        saveApiKeyConfig(systemConfig, params);
        // 修改RSA配置
        saveRSAConfig(systemConfig, params);
        // 修改 IP 白名单配置
        saveIpWhitelistConfig(systemConfig, params);
        logService.log(LogType.SystemLog.INFO, "修改系统配置");

        // 修改基本配置
        this.modifyConfigModel(systemConfig, params);
        return systemConfig;
    }

    private void saveApiKeyConfig(SystemConfig systemConfig, Map<String, String> params) {
        if (!systemConfig.isEnableOpenAPI()) {
            return;
        }
        String apiSecret = params.get("apiSecret");
        Assert.hasText(apiSecret, "API密钥不能为空");
        ApiKeyConfig apiKeyConfig = apiKeyManager.addCredential(systemConfig.getApiKeyConfig(), apiSecret);
        systemConfig.setApiKeyConfig(apiKeyConfig);
    }

    private void saveIpWhitelistConfig(SystemConfig systemConfig, Map<String, String> params) {
        if (!systemConfig.isEnableOpenAPI()) {
            return;
        }
        IpWhitelistConfig config = new IpWhitelistConfig();
        config.setEnabled(StringUtil.isNotBlank(params.get("ipWhitelistEnabled")));
        config.setAllowLocalhost(StringUtil.isNotBlank(params.get("ipWhitelistAllowLocalhost")));
        String ipWhitelist = params.get("ipWhitelist");
        if (StringUtil.isNotBlank(ipWhitelist)) {
            List<String> list = Arrays.stream(ipWhitelist.split("[,\n\r]+"))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .distinct()
                    .collect(Collectors.toList());
            config.setWhitelist(list);
        }
        systemConfig.setIpWhitelistConfig(config);
    }

    private void saveRSAConfig(SystemConfig systemConfig, Map<String, String> params) {
        if (!systemConfig.isEnableOpenAPI()) {
            return;
        }
        String publicKey = params.get("rsaPublicKey");
        String privateKey = params.get("rsaPrivateKey");
        String rsaKeyLength = params.get("rsaKeyLength");
        Assert.hasText(publicKey, "RSA公钥不能为空");
        Assert.hasText(privateKey, "RSA私钥不能为空");
        Assert.hasText(rsaKeyLength, "密钥长度不能为空");
        int keyLength = NumberUtil.toInt(rsaKeyLength);
        Assert.isTrue(keyLength >= 1024 && keyLength <= 8192, "密钥长度支持的范围[1024-8192]");
        RsaConfig rsaConfig = rsaManager.addCredential(systemConfig.getRsaConfig(), publicKey, privateKey, keyLength);
        systemConfig.setRsaConfig(rsaConfig);
    }

}
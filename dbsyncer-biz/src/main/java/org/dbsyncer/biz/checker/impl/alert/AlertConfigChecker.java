/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.alert;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.AlertChannelHttp;
import org.dbsyncer.parser.model.AlertChannelMail;
import org.dbsyncer.parser.model.AlertChannelWeChat;
import org.dbsyncer.parser.model.AlertConfig;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.SystemConfig;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 告警配置校验器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-03-04 19:00
 */
@Component
public class AlertConfigChecker extends AbstractChecker {

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        throw new BizException("Unsupported method");
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        printParams(params);
        Assert.notEmpty(params, "Config check params is null.");
        SystemConfig systemConfig = profileComponent.getSystemConfig();
        Assert.notNull(systemConfig, "配置文件为空.");

        if (systemConfig.getAlertConfig() == null) {
            systemConfig.setAlertConfig(new AlertConfig());
        }
        AlertConfig alertConfig = systemConfig.getAlertConfig();

        // 是否启用企业微信告警
        boolean enableWechat = StringUtil.isNotBlank(params.get("enableWechat"));
        if (enableWechat) {
            AlertChannelWeChat config = new AlertChannelWeChat();
            config.setWebhookUrl(params.get("webhookUrl"));
            config.setAtAll(StringUtil.isNotBlank(params.get("wechatAtAll")));
            config.setAtUsers(params.get("wechatAtUsers"));
            alertConfig.setWeiXin(config);
        } else {
            alertConfig.setWeiXin(null);
        }

        // 是否启用HTTP告警
        boolean enableHttp = StringUtil.isNotBlank(params.get("enableHttp"));
        if (enableHttp) {
            AlertChannelHttp config = new AlertChannelHttp();
            config.setUrl(params.get("httpUrl"));
            alertConfig.setHttp(config);
        } else {
            alertConfig.setHttp(null);
        }

        // 是否启用邮件告警
        boolean enableMail = StringUtil.isNotBlank(params.get("enableMail"));
        if (enableMail) {
            AlertChannelMail config = new AlertChannelMail();
            config.setAccount(params.get("mailAccount"));
            config.setCode(params.get("mailCode"));
            alertConfig.setMail(config);
        } else {
            alertConfig.setMail(null);
        }
        return systemConfig;
    }
}

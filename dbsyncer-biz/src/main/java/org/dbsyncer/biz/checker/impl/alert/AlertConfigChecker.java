/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.alert;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.plugin.model.AlertChannelHttp;
import org.dbsyncer.plugin.model.AlertChannelMail;
import org.dbsyncer.plugin.model.AlertChannelWeChat;
import org.dbsyncer.plugin.model.AlertConfig;
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
        AlertChannelWeChat weChat = alertConfig.getWechat();
        weChat.setEnabled(enableWechat);
        if (enableWechat) {
            String webhookUrl = params.get("wechatWebhookUrl");
            Assert.hasText(webhookUrl, "企业微信告警Webhook地址不能为空.");
            weChat.setWebhookUrl(webhookUrl);
            weChat.setAtAll(StringUtil.isNotBlank(params.get("wechatAtAll")));
            weChat.setAtUserMobiles(params.get("wechatAtUserMobiles"));
        }

        // 是否启用HTTP告警
        boolean enableHttp = StringUtil.isNotBlank(params.get("enableHttp"));
        AlertChannelHttp http = alertConfig.getHttp();
        http.setEnabled(enableHttp);
        if (enableHttp) {
            String url = params.get("httpUrl");
            Assert.hasText(url, "回调HTTP地址不能为空.");
            http.setUrl(url);
        }

        // 是否启用邮件告警
        boolean enableMail = StringUtil.isNotBlank(params.get("enableMail"));
        AlertChannelMail mail = alertConfig.getMail();
        mail.setEnabled(enableMail);
        if (enableMail) {
            String account = params.get("mailAccount");
            String code = params.get("mailCode");
            Assert.hasText(account, "账号不能为空.");
            Assert.hasText(code, "Code不能为空.");
            mail.setAccount(account);
            mail.setCode(code);
        }
        return systemConfig;
    }
}

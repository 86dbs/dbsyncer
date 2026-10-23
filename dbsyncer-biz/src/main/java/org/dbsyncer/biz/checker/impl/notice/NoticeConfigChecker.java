/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.notice;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.model.DingTalkNoticeChannel;
import org.dbsyncer.common.model.HttpNoticeChannel;
import org.dbsyncer.common.model.MailNoticeChannel;
import org.dbsyncer.common.model.WeChatNoticeChannel;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.message.DefaultNoticeStrategy;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.model.NoticeStrategyDef;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 通知配置校验器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026-03-04 19:00
 */
@Component
public class NoticeConfigChecker extends AbstractChecker {

    @Resource
    private SystemConfigProfile systemConfigProfile;

    @Override
    public SystemConfig checkAddConfigModel(Map<String, String> params) {
        throw new BizException("Unsupported method");
    }

    @Override
    public SystemConfig checkEditConfigModel(Map<String, String> params) {
        printParams(params);
        Assert.notEmpty(params, "Config check params is null.");
        SystemConfig systemConfig = systemConfigProfile.getSystemConfig();
        Assert.notNull(systemConfig, "配置文件为空.");

        if (systemConfig.getNoticeConfig() == null) {
            systemConfig.setNoticeConfig(new NoticeConfig());
        }
        NoticeConfig noticeConfig = systemConfig.getNoticeConfig();

        // 是否启用企业微信告警
        boolean enableWechat = StringUtil.isNotBlank(params.get("enableWechat"));
        WeChatNoticeChannel weChat = noticeConfig.getWechat();
        weChat.setEnabled(enableWechat);
        if (enableWechat) {
            String webhookUrl = StringUtil.trim(params.get("wechatWebhookUrl"));
            Assert.hasText(webhookUrl, "企业微信告警Webhook地址不能为空.");
            weChat.setWebhookUrl(webhookUrl);
            weChat.setAtAll(StringUtil.isNotBlank(params.get("wechatAtAll")));
            weChat.setAtMobiles(params.get("wechatAtMobiles"));
        }

        // 是否启用钉钉告警
        boolean enableDingTalk = StringUtil.isNotBlank(params.get("enableDingTalk"));
        DingTalkNoticeChannel dingTalk = noticeConfig.getDingTalk();
        dingTalk.setEnabled(enableDingTalk);
        if (enableDingTalk) {
            String webhookUrl = StringUtil.trim(params.get("dingTalkWebhookUrl"));
            Assert.hasText(webhookUrl, "钉钉告警Webhook地址不能为空.");
            dingTalk.setWebhookUrl(webhookUrl);
            dingTalk.setAtAll(StringUtil.isNotBlank(params.get("dingTalkAtAll")));
            dingTalk.setAtMobiles(params.get("dingTalkAtMobiles"));
        }

        // 是否启用HTTP告警
        boolean enableHttp = StringUtil.isNotBlank(params.get("enableHttp"));
        HttpNoticeChannel http = noticeConfig.getHttp();
        http.setEnabled(enableHttp);
        if (enableHttp) {
            String url = StringUtil.trim(params.get("httpUrl"));
            Assert.hasText(url, "回调HTTP地址不能为空.");
            http.setUrl(url);
        }

        // 是否启用邮件告警
        boolean enableMail = StringUtil.isNotBlank(params.get("enableMail"));
        MailNoticeChannel mail = noticeConfig.getMail();
        mail.setEnabled(enableMail);
        if (enableMail) {
            String account = params.get("mailAccount");
            String code = params.get("mailCode");
            Assert.hasText(account, "账号不能为空.");
            Assert.hasText(code, "Code不能为空.");
            mail.setAccount(account);
            mail.setCode(code);
        }

        noticeConfig.setStrategies(parseStrategies(params.get("strategiesJson")));
        return systemConfig;
    }

    private List<NoticeStrategyDef> parseStrategies(String json) {
        if (StringUtil.isBlank(json)) {
            return DefaultNoticeStrategy.allDefs();
        }
        List<NoticeStrategyDef> list = JsonUtil.jsonToArray(json, NoticeStrategyDef.class);
        if (CollectionUtils.isEmpty(list)) {
            return DefaultNoticeStrategy.allDefs();
        }
        Set<String> ids = new HashSet<>();
        for (NoticeStrategyDef def : list) {
            Assert.hasText(def.getId(), "策略ID不能为空");
            Assert.hasText(def.getName(), "策略名称不能为空");
            Assert.isTrue(ids.add(def.getId()), "策略ID重复: " + def.getId());
            NoticeFrequencyEnum frequency;
            try {
                frequency = NoticeFrequencyEnum.valueOf(def.getFrequency());
            } catch (Exception e) {
                throw new BizException("策略频率无效: " + def.getFrequency());
            }
            if (frequency == NoticeFrequencyEnum.INTERVAL) {
                Assert.hasText(def.getCron(), "周期策略须填写 Cron: " + def.getName());
            }
            Assert.isTrue(!CollectionUtils.isEmpty(def.getNoticeTypes()), "策略须至少选择一种通知类型: " + def.getName());
        }
        return list;
    }
}

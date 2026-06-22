/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.message.NoticeChannelDispatcher;
import org.dbsyncer.plugin.impl.AbstractNoticeService;
import org.dbsyncer.plugin.model.TestNoticeContent;
import org.dbsyncer.sdk.enums.NoticeChannelEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.notice.MessageService;
import org.dbsyncer.sdk.notice.NoticeDecisionEngine;
import org.dbsyncer.sdk.notice.NoticeService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class MessageServiceImpl implements MessageService {

    @Resource
    private NoticeChannelDispatcher noticeChannelDispatcher;

    @Resource
    private NoticeDecisionEngine noticeDecisionEngine;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private AppConfig appConfig;

    @Override
    public void sendMessage(NoticeContent noticeContent) {
        NoticeConfig noticeConfig = profileComponent.getSystemConfig().getNoticeConfig();
        if (noticeConfig == null || noticeContent == null) {
            return;
        }
        if (noticeContent.getNoticeType() == NoticeTypeEnum.TEST_MESSAGE) {
            noticeChannelDispatcher.deliver(noticeContent);
            return;
        }
        noticeDecisionEngine.dispatch(noticeContent);
    }

    @Override
    public String testSendMessage() {
        NoticeConfig noticeConfig = profileComponent.getSystemConfig().getNoticeConfig();
        if (noticeConfig == null) {
            throw new ParserException("请先保存告警配置");
        }
        TestNoticeContent noticeContent = new TestNoticeContent();
        noticeContent.setTitle("手动测试消息");
        noticeContent.setContent("告警通知配置成功");
        sendMessage(noticeContent);
        return "success";
    }

    @Override
    public void registerNotifyService(NoticeChannelEnum noticeChannelEnum, NoticeService notificationService) {
        if (notificationService instanceof AbstractNoticeService) {
            AbstractNoticeService ans = (AbstractNoticeService) notificationService;
            ans.setAppConfig(appConfig);
        }
        noticeChannelDispatcher.registerNotifyService(noticeChannelEnum, notificationService);
    }

    @Override
    public void removeNotifyService(NoticeChannelEnum noticeChannelEnum) {
        noticeChannelDispatcher.removeNotifyService(noticeChannelEnum);
    }
}

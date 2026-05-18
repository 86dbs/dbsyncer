/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.plugin.impl.AbstractNoticeService;
import org.dbsyncer.plugin.model.TestNoticeContent;
import org.dbsyncer.sdk.enums.NoticeChannelEnum;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeMessage;
import org.dbsyncer.sdk.notice.MessageService;
import org.dbsyncer.sdk.notice.NoticeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MessageServiceImpl implements MessageService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<NoticeChannelEnum, NoticeService> notifyServiceMap = new ConcurrentHashMap<>();

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
        NoticeMessage message = new NoticeMessage();
        message.setNoticeConfig(noticeConfig);
        message.setNoticeContent(noticeContent);
        notifyServiceMap.forEach((channel, service) -> {
            try {
                // 如果是邮件渠道，设置邮件接收人
                setMessageReceiversIfMail(channel, message);
                service.notify(message);
            } catch (Exception e) {
                logger.error("Send message error, channel: {}", channel, e);
                throw new ParserException(e);
            }
        });
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
        this.notifyServiceMap.put(noticeChannelEnum, notificationService);
    }

    @Override
    public void removeNotifyService(NoticeChannelEnum noticeChannelEnum) {
        this.notifyServiceMap.remove(noticeChannelEnum);
    }

    private void setMessageReceiversIfMail(NoticeChannelEnum channel, NoticeMessage message) {
        if (channel != NoticeChannelEnum.EMAIL) {
            return;
        }
        UserConfig userConfig = profileComponent.getUserConfig();
        if (null == userConfig) {
            return;
        }

        List<String> mails = new ArrayList<>();
        userConfig.getUserInfoList().forEach(userInfo -> {
            if (StringUtil.isNotBlank(userInfo.getEmail())) {
                mails.addAll(Arrays.asList(StringUtil.split(userInfo.getEmail(), StringUtil.COMMA)));
            }
        });
        if (!CollectionUtils.isEmpty(mails)) {
            message.setReceivers(mails);
        }
    }
}
/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.enums.NoticeChannelEnum;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeMessage;
import org.dbsyncer.sdk.notice.NoticeDeliveryHandler;
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

/**
 * 将通知投递到全局已启用渠道。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
@Component
public class NoticeChannelDispatcher implements NoticeDeliveryHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<NoticeChannelEnum, NoticeService> notifyServiceMap = new ConcurrentHashMap<>();

    @Resource
    private ProfileComponent profileComponent;

    public void registerNotifyService(NoticeChannelEnum noticeChannelEnum, NoticeService notificationService) {
        notifyServiceMap.put(noticeChannelEnum, notificationService);
    }

    public void removeNotifyService(NoticeChannelEnum noticeChannelEnum) {
        notifyServiceMap.remove(noticeChannelEnum);
    }

    @Override
    public void deliver(NoticeContent noticeContent) {
        NoticeConfig noticeConfig = profileComponent.getSystemConfig().getNoticeConfig();
        if (noticeConfig == null || noticeContent == null || notifyServiceMap.isEmpty()) {
            return;
        }
        NoticeMessage message = new NoticeMessage();
        message.setNoticeConfig(noticeConfig);
        message.setNoticeContent(noticeContent);
        notifyServiceMap.forEach((channel, service) -> {
            try {
                setMessageReceiversIfMail(channel, message);
                service.notify(message);
            } catch (Exception e) {
                logger.error("Send message error, channel: {}", channel, e);
                throw new ParserException(e);
            }
        });
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

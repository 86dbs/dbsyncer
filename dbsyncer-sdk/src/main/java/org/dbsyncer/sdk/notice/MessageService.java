package org.dbsyncer.sdk.notice;

import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.enums.NoticeChannelEnum;

public interface MessageService {

    /**
     * 发送消息
     *
     * @param noticeContent
     */
    void sendMessage(NoticeContent noticeContent);

    /**
     * 测试发送消息
     */
    String testSendMessage();

    /**
     * 注册通知服务
     *
     * @param noticeChannelEnum
     * @param notificationService
     */
    void registerNotifyService(NoticeChannelEnum noticeChannelEnum, NoticeService notificationService);

    /**
     * 移除通知服务
     *
     * @param noticeChannelEnum
     */
    void removeNotifyService(NoticeChannelEnum noticeChannelEnum);
}

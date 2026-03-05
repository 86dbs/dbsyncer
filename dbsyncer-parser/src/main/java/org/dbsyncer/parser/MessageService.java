package org.dbsyncer.parser;

import org.dbsyncer.plugin.NoticeService;
import org.dbsyncer.plugin.enums.NoticeChannelEnum;

public interface MessageService {

    /**
     * 发送消息
     *
     * @param title
     * @param content
     */
    void sendMessage(String title, String content);

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

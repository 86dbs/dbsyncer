/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;



import java.util.List;

/**
 * 通知消息
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/03/05 22:14
 */
public class NoticeMessage {

    /**
     * 通知内容
     */
    private NoticeContent noticeContent;

    /**
     * 消息接收人
     */
    private List<String> receivers;

    /**
     * 通知配置
     */
    private NoticeConfig noticeConfig;

    public NoticeContent getNoticeContent() {
        return noticeContent;
    }

    public void setNoticeContent(NoticeContent noticeContent) {
        this.noticeContent = noticeContent;
    }

    public List<String> getReceivers() {
        return receivers;
    }

    public void setReceivers(List<String> receivers) {
        this.receivers = receivers;
    }

    public NoticeConfig getNoticeConfig() {
        return noticeConfig;
    }

    public void setNoticeConfig(NoticeConfig noticeConfig) {
        this.noticeConfig = noticeConfig;
    }
}

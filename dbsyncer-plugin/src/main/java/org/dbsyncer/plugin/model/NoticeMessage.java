/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;

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
     * 消息标题
     */
    private String title;

    /**
     * 消息内容
     */
    private String content;

    /**
     * 消息接收人
     */
    private List<String> receivers;

    /**
     * 告警配置
     */
    private NoticeConfig alertConfig;

    public static NoticeMessage newBuilder() {
        return new NoticeMessage();
    }

    public String getTitle() {
        return title;
    }

    public NoticeMessage setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getContent() {
        return content;
    }

    public NoticeMessage setContent(String content) {
        this.content = content;
        return this;
    }

    public List<String> getReceivers() {
        return receivers;
    }

    public NoticeMessage setReceivers(List<String> receivers) {
        this.receivers = receivers;
        return this;
    }

    public NoticeConfig getAlertConfig() {
        return alertConfig;
    }

    public NoticeMessage setAlertConfig(NoticeConfig alertConfig) {
        this.alertConfig = alertConfig;
        return this;
    }
}

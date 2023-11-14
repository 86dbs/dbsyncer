package org.dbsyncer.sdk.model;

import java.util.List;

/**
 * 通知消息
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/13 22:14
 */
public class NotifyMessage {

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

    public static NotifyMessage newBuilder() {
        return new NotifyMessage();
    }

    public String getTitle() {
        return title;
    }

    public NotifyMessage setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getContent() {
        return content;
    }

    public NotifyMessage setContent(String content) {
        this.content = content;
        return this;
    }

    public List<String> getReceivers() {
        return receivers;
    }

    public NotifyMessage setReceivers(List<String> receivers) {
        this.receivers = receivers;
        return this;
    }
}
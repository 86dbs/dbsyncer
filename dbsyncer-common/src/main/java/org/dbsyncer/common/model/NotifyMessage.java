package org.dbsyncer.common.model;

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
    private String receiver;

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

    public String getReceiver() {
        return receiver;
    }

    public NotifyMessage setReceiver(String receiver) {
        this.receiver = receiver;
        return this;
    }
}
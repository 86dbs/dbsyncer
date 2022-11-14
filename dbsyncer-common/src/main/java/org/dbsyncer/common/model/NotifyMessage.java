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

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getReceiver() {
        return receiver;
    }

    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }
}
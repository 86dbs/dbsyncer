package org.dbsyncer.parser.message;

public interface MessageService {

    /**
     * 发送消息
     *
     * @param title
     * @param content
     */
    void sendMessage(String title, String content);
}
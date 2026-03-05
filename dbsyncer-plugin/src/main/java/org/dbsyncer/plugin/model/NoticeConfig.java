/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;

/**
 * 通知配置
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-03-04 19:00
 */
public class NoticeConfig {

    private final WeChatNoticeChannel wechat = new WeChatNoticeChannel();
    private final HttpNoticeChannel http = new HttpNoticeChannel();
    private final MailNoticeChannel mail = new MailNoticeChannel();

    public WeChatNoticeChannel getWechat() {
        return wechat;
    }

    public HttpNoticeChannel getHttp() {
        return http;
    }

    public MailNoticeChannel getMail() {
        return mail;
    }

}

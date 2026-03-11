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

    private WeChatNoticeChannel wechat = new WeChatNoticeChannel();
    private DingTalkNoticeChannel dingTalk = new DingTalkNoticeChannel();
    private HttpNoticeChannel http = new HttpNoticeChannel();
    private MailNoticeChannel mail = new MailNoticeChannel();

    public WeChatNoticeChannel getWechat() {
        return wechat;
    }

    public void setWechat(WeChatNoticeChannel wechat) {
        this.wechat = wechat;
    }

    public DingTalkNoticeChannel getDingTalk() {
        return dingTalk;
    }

    public void setDingTalk(DingTalkNoticeChannel dingTalk) {
        this.dingTalk = dingTalk;
    }

    public HttpNoticeChannel getHttp() {
        return http;
    }

    public void setHttp(HttpNoticeChannel http) {
        this.http = http;
    }

    public MailNoticeChannel getMail() {
        return mail;
    }

    public void setMail(MailNoticeChannel mail) {
        this.mail = mail;
    }
}

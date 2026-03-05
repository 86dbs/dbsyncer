/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

/**
 * 告警配置
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-03-04 19:00
 */
public class AlertConfig {

    private AlertChannelWeChat wechat;
    private AlertChannelHttp http;
    private AlertChannelMail mail;

    public AlertChannelWeChat getWechat() {
        return wechat;
    }

    public void setWechat(AlertChannelWeChat wechat) {
        this.wechat = wechat;
    }

    public AlertChannelHttp getHttp() {
        return http;
    }

    public void setHttp(AlertChannelHttp http) {
        this.http = http;
    }

    public AlertChannelMail getMail() {
        return mail;
    }

    public void setMail(AlertChannelMail mail) {
        this.mail = mail;
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;

/**
 * 告警配置
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-03-04 19:00
 */
public class AlertConfig {

    private AlertChannelWeChat wechat = new AlertChannelWeChat();
    private AlertChannelHttp http = new AlertChannelHttp();
    private AlertChannelMail mail = new AlertChannelMail();

    public AlertChannelWeChat getWechat() {
        return wechat;
    }

    public AlertChannelHttp getHttp() {
        return http;
    }

    public AlertChannelMail getMail() {
        return mail;
    }

}

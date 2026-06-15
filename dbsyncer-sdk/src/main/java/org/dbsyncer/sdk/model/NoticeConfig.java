/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.model.DingTalkNoticeChannel;
import org.dbsyncer.common.model.HttpNoticeChannel;
import org.dbsyncer.common.model.MailNoticeChannel;
import org.dbsyncer.common.model.WeChatNoticeChannel;

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

    /**
     * 同步任务有失败数据
     */
    private boolean enableMappingError = true;
    /**
     * 停止驱动
     */
    private boolean enableMappingStop = true;
    /**
     * 连接离线
     */
    private boolean enableConnectorOffline = true;
    /**
     * 订正校验失败
     */
    private boolean enableValidateSyncFail = true;
    /**
     * 系统消息
     */
    private boolean enableSystemMessage = true;

    public org.dbsyncer.common.model.WeChatNoticeChannel getWechat() {
        return wechat;
    }

    public void setWechat(org.dbsyncer.common.model.WeChatNoticeChannel wechat) {
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

    public boolean isEnableMappingError() {
        return enableMappingError;
    }

    public void setEnableMappingError(boolean enableMappingError) {
        this.enableMappingError = enableMappingError;
    }

    public boolean isEnableMappingStop() {
        return enableMappingStop;
    }

    public void setEnableMappingStop(boolean enableMappingStop) {
        this.enableMappingStop = enableMappingStop;
    }

    public boolean isEnableConnectorOffline() {
        return enableConnectorOffline;
    }

    public void setEnableConnectorOffline(boolean enableConnectorOffline) {
        this.enableConnectorOffline = enableConnectorOffline;
    }

    public boolean isEnableValidateSyncFail() {
        return enableValidateSyncFail;
    }

    public void setEnableValidateSyncFail(boolean enableValidateSyncFail) {
        this.enableValidateSyncFail = enableValidateSyncFail;
    }

    public boolean isEnableSystemMessage() {
        return enableSystemMessage;
    }

    public void setEnableSystemMessage(boolean enableSystemMessage) {
        this.enableSystemMessage = enableSystemMessage;
    }
}

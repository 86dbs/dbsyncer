/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

/**
 * 钉钉告警配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026-03-04 19:00
 */
public final class DingTalkNoticeChannel extends NoticeChannel {

    /**
     * 机器人webhook地址
     */
    private String webhookUrl;

    /**
     * 是否@所有人(与@用户手机号功能互斥)
     */
    private boolean atAll;

    /**
     * @用户手机号，多个用逗号分隔
     */
    private String atMobiles;

    public String getWebhookUrl() {
        return webhookUrl;
    }

    public void setWebhookUrl(String webhookUrl) {
        this.webhookUrl = webhookUrl;
    }

    public boolean isAtAll() {
        return atAll;
    }

    public void setAtAll(boolean atAll) {
        this.atAll = atAll;
    }

    public String getAtMobiles() {
        return atMobiles;
    }

    public void setAtMobiles(String atMobiles) {
        this.atMobiles = atMobiles;
    }
}

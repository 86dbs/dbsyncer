/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

/**
 * 机器人告警渠道（企业微信 / 钉钉等）：webhook；发送时的 @ 由通知规则覆盖写入。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-23
 */
public abstract class RobotNoticeChannel extends NoticeChannel {

    /**
     * 机器人 webhook 地址
     */
    private String webhookUrl;

    /**
     * 是否 @所有人
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

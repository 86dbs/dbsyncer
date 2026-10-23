/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 告警策略配置（表 dbsyncer_notice_strategy）。
 *
 * @author wuji
 * @version 1.0.0
 */
public class NoticeStrategyConfig extends ConfigModel {

    /**
     * 是否启用：0 否 / 1 是
     */
    private int enabled = 1;

    /**
     * {@link org.dbsyncer.sdk.enums.NoticeFrequencyEnum} 名称
     */
    private String frequency;

    /**
     * 周期 Cron（INTERVAL 时必填）
     */
    private String cron;

    /**
     * 通知类型码，多个逗号分隔
     */
    private String noticeTypes;

    public NoticeStrategyConfig() {
        super.setName("");
        super.setType(ConfigConstant.NOTICE_STRATEGY);
    }

    public int getEnabled() {
        return enabled;
    }

    public void setEnabled(int enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled == 1;
    }

    public String getFrequency() {
        return frequency;
    }

    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getNoticeTypes() {
        return noticeTypes;
    }

    public void setNoticeTypes(String noticeTypes) {
        this.noticeTypes = noticeTypes;
    }
}

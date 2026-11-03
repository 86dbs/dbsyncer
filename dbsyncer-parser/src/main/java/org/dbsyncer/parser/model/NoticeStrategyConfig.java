/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 告警策略配置（表 dbsyncer_notice_strategy），每种通知类型一行。
 *
 * @author wuji
 * @version 1.0.0
 */
public class NoticeStrategyConfig extends ConfigModel {

    /**
     * 通知类型码，存 {@link org.dbsyncer.sdk.enums.NoticeTypeEnum#getCode()}
     */
    private int noticeTypes;

    /**
     * 是否启用：0 否 / 1 是
     */
    private int enabled = 1;

    /**
     * 频率：0-IMMEDIATE / 1-INTERVAL
     */
    private int frequency;

    /**
     * 周期 Cron（INTERVAL 时必填）
     */
    private String cron;

    /**
     * 备注说明
     */
    private String remark;

    public NoticeStrategyConfig() {
        super.setName("");
        super.setType(ConfigConstant.NOTICE_STRATEGY);
    }

    public int getNoticeTypes() {
        return noticeTypes;
    }

    public void setNoticeTypes(int noticeTypes) {
        this.noticeTypes = noticeTypes;
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

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}

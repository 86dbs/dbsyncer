/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

/**
 * 可持久化的告警策略定义（DTO，对应表 dbsyncer_notice_strategy）。
 *
 * @author wuji
 * @version 1.0.0
 */
public class NoticeStrategyDef {

    private String id;

    private String name;

    /**
     * 是否启用
     */
    private boolean enabled = true;

    /**
     * {@link org.dbsyncer.sdk.enums.NoticeFrequencyEnum#getCode()}
     */
    private int frequency;

    /**
     * 周期 Cron（INTERVAL 时必填）
     */
    private String cron;

    /**
     * {@link org.dbsyncer.sdk.enums.NoticeTypeEnum#getCode()}
     */
    private int noticeTypes;

    /**
     * 备注说明
     */
    private String remark;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
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

    public int getNoticeTypes() {
        return noticeTypes;
    }

    public void setNoticeTypes(int noticeTypes) {
        this.noticeTypes = noticeTypes;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.ArrayList;
import java.util.List;

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
     * {@link org.dbsyncer.sdk.enums.NoticeFrequencyEnum} 名称
     */
    private String frequency;

    /**
     * 周期 Cron（INTERVAL 时必填）
     */
    private String cron;

    /**
     * 通知类型码列表（{@link org.dbsyncer.sdk.enums.NoticeTypeEnum#name()}）
     */
    private List<String> noticeTypes = new ArrayList<>();

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

    public List<String> getNoticeTypes() {
        return noticeTypes;
    }

    public void setNoticeTypes(List<String> noticeTypes) {
        this.noticeTypes = noticeTypes;
    }
}

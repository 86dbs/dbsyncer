/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;

/**
 * 告警通知策略（运行时，每种通知类型对应一条）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public final class NoticeStrategy {

    private final String id;
    private final String name;
    private final NoticeFrequencyEnum frequency;
    private final String cron;
    private final NoticeTypeEnum noticeType;

    public NoticeStrategy(String id, String name, NoticeFrequencyEnum frequency, String cron,
                          NoticeTypeEnum noticeType) {
        this.id = id;
        this.name = name;
        this.frequency = frequency;
        this.cron = cron;
        this.noticeType = noticeType;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public NoticeFrequencyEnum getFrequency() {
        return frequency;
    }

    public String getCron() {
        return cron;
    }

    public NoticeTypeEnum getNoticeType() {
        return noticeType;
    }

    public boolean supports(NoticeTypeEnum type) {
        return type != null && this.noticeType == type;
    }

    public boolean isImmediate() {
        return frequency == NoticeFrequencyEnum.IMMEDIATE;
    }

    public boolean isInterval() {
        return frequency == NoticeFrequencyEnum.INTERVAL;
    }

    public boolean hasCron() {
        return isInterval() && cron != null && !cron.isEmpty();
    }
}

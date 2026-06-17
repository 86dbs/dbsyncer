/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * 告警通知策略（本版内置，后续可扩展为用户自定义）
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
    private final Set<NoticeTypeEnum> noticeTypes;

    public NoticeStrategy(String id, String name, NoticeFrequencyEnum frequency, String cron,
                          Set<NoticeTypeEnum> noticeTypes) {
        this.id = id;
        this.name = name;
        this.frequency = frequency;
        this.cron = cron;
        this.noticeTypes = Collections.unmodifiableSet(EnumSet.copyOf(noticeTypes));
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

    public Set<NoticeTypeEnum> getNoticeTypes() {
        return noticeTypes;
    }

    public boolean supports(NoticeTypeEnum noticeType) {
        return noticeType != null && noticeTypes.contains(noticeType);
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

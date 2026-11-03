/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 告警策略发送频率：0-立即 / 1-周期。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public enum NoticeFrequencyEnum {

    /**
     * 立即发送
     */
    IMMEDIATE(0),

    /**
     * 周期发送（由 Cron 表达式调度 flush）
     */
    INTERVAL(1);

    private final int code;

    NoticeFrequencyEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    /**
     * 按编码解析；未知返回 {@code null}。
     */
    public static NoticeFrequencyEnum getByCode(int code) {
        for (NoticeFrequencyEnum frequency : values()) {
            if (frequency.code == code) {
                return frequency;
            }
        }
        return null;
    }
}

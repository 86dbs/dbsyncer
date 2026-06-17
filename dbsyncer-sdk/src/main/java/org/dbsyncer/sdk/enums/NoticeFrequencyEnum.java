/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 告警策略发送频率
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public enum NoticeFrequencyEnum {

    /**
     * 立即发送
     */
    IMMEDIATE,

    /**
     * 周期发送（由 Cron 表达式调度 flush）
     */
    INTERVAL
}

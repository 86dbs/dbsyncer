/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeStrategy;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * 内置默认告警策略。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public final class DefaultNoticeStrategy {

    public static final String INTERVAL_10M = "interval_10m";
    public static final String IMMEDIATE = "immediate";
    public static final String INTERVAL_1D = "interval_1d";

    private static final String CRON_EVERY_10M = "0 */10 * * * ?";
    private static final String CRON_DAILY_9AM = "0 0 9 * * ?";

    private DefaultNoticeStrategy() {
    }

    public static List<NoticeStrategy> all() {
        return Arrays.asList(interval10m(), immediate(), interval1d());
    }

    public static NoticeStrategy interval10m() {
        return new NoticeStrategy(INTERVAL_10M, "10分钟一次", NoticeFrequencyEnum.INTERVAL, CRON_EVERY_10M,
                types(NoticeTypeEnum.LICENSE_EXCEPTION));
    }

    public static NoticeStrategy immediate() {
        return new NoticeStrategy(IMMEDIATE, "立即发送", NoticeFrequencyEnum.IMMEDIATE, null,
                types(NoticeTypeEnum.VALIDATE_SYNC_FAIL, NoticeTypeEnum.MAPPING_STOP,
                        NoticeTypeEnum.MAPPING_ERROR, NoticeTypeEnum.CONNECTOR_OFFLINE));
    }

    public static NoticeStrategy interval1d() {
        return new NoticeStrategy(INTERVAL_1D, "每天一次", NoticeFrequencyEnum.INTERVAL, CRON_DAILY_9AM,
                types(NoticeTypeEnum.LICENSE_EXPIRE_REMIND, NoticeTypeEnum.LICENSE_EXPIRED));
    }

    private static Set<NoticeTypeEnum> types(NoticeTypeEnum... noticeTypes) {
        return EnumSet.copyOf(Arrays.asList(noticeTypes));
    }
}

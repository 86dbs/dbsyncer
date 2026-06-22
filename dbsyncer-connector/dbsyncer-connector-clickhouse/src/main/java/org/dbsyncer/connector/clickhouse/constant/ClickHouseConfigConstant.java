/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.clickhouse.constant;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:50
 */
public final class ClickHouseConfigConstant {

    private ClickHouseConfigConstant() {
    }

    /**
     * 增量轮询间隔（秒）
     */
    public static final String POLL_INTERVAL_SECONDS = "pollIntervalSeconds";

    /**
     * 增量批次大小
     */
    public static final String POLL_BATCH_SIZE = "pollBatchSize";
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.model.DatabaseMigrationSyncTask;

import java.math.BigDecimal;

/**
 * 整库迁移任务列表 VO
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 00:00
 */
public final class DatabaseSyncTaskVO extends DatabaseMigrationSyncTask {

    private final Connector sourceConnector;
    private final Connector targetConnector;
    private int mappingCount;
    /** 任务进度 0~100，运行中由快照计算 */
    private BigDecimal progress;
    /** 失败明细条数（failTotal > 0） */
    private long errorCount;

    public DatabaseSyncTaskVO(Connector sourceConnector, Connector targetConnector) {
        this.sourceConnector = sourceConnector;
        this.targetConnector = targetConnector;
    }

    public Connector getSourceConnector() {
        return sourceConnector;
    }

    public Connector getTargetConnector() {
        return targetConnector;
    }

    public int getMappingCount() {
        return mappingCount;
    }

    public void setMappingCount(int mappingCount) {
        this.mappingCount = mappingCount;
    }

    public BigDecimal getProgress() {
        return progress;
    }

    public void setProgress(BigDecimal progress) {
        this.progress = progress;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }
}

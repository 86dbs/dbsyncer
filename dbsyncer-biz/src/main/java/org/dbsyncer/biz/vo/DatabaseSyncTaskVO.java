/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.DatabaseSyncTask;

import java.math.BigDecimal;
import java.util.List;

/**
 * 整库迁移任务列表 VO
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 00:00
 */
public final class DatabaseSyncTaskVO extends DatabaseSyncTask {

    private final Connector sourceConnector;
    private final Connector targetConnector;

    /**
     * 库映射视图（由 table_group 还原，仅展示/编辑用，不落 task JSON）
     */
    private List<DatabaseMapping> databaseMappings;

    private int mappingCount;
    /** 任务进度 0~100，运行中由快照计算 */
    private BigDecimal progress;
    /** 失败明细条数（failTotal > 0） */
    private long errorCount;
    /** 任务总表数（TableGroup 数量） */
    private int totalTableCount;
    /** 已完成表数（运行快照中已全部完成的表） */
    private int completedTableCount;

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

    public List<DatabaseMapping> getDatabaseMappings() {
        return databaseMappings;
    }

    public void setDatabaseMappings(List<DatabaseMapping> databaseMappings) {
        this.databaseMappings = databaseMappings;
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

    public int getTotalTableCount() {
        return totalTableCount;
    }

    public void setTotalTableCount(int totalTableCount) {
        this.totalTableCount = totalTableCount;
    }

    public int getCompletedTableCount() {
        return completedTableCount;
    }

    public void setCompletedTableCount(int completedTableCount) {
        this.completedTableCount = completedTableCount;
    }
}

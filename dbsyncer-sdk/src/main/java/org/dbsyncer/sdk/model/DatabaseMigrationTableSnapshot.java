/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.enums.MigrationStepStatusEnum;

import java.io.Serializable;

/**
 * 表级迁移快照（按表映射 index 索引）。
 * <p>整库迁移分结构、数据两阶段；数据阶段用 {@link #dataCursor} 断点续传。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 11:30
 */
public class DatabaseMigrationTableSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 结构迁移状态，见 {@link MigrationStepStatusEnum} */
    private int schemaStatus;

    /** 数据迁移分页游标（页码，从 1 开始） */
    private long dataCursor;

    /** 数据迁移状态，见 {@link MigrationStepStatusEnum} */
    private int dataStatus;

    public DatabaseMigrationTableSnapshot() {
    }

    public DatabaseMigrationTableSnapshot(int schemaStatus, long dataCursor, int dataStatus) {
        this.schemaStatus = schemaStatus;
        this.dataCursor = dataCursor;
        this.dataStatus = dataStatus;
    }

    public int getSchemaStatus() {
        return schemaStatus;
    }

    public void setSchemaStatus(int schemaStatus) {
        this.schemaStatus = schemaStatus;
    }

    public void setSchemaStatus(MigrationStepStatusEnum schemaStatus) {
        this.schemaStatus = schemaStatus == null ? MigrationStepStatusEnum.PENDING.getCode() : schemaStatus.getCode();
    }

    public long getDataCursor() {
        return dataCursor;
    }

    public void setDataCursor(long dataCursor) {
        this.dataCursor = dataCursor;
    }

    public int getDataStatus() {
        return dataStatus;
    }

    public void setDataStatus(int dataStatus) {
        this.dataStatus = dataStatus;
    }

    public void setDataStatus(MigrationStepStatusEnum dataStatus) {
        this.dataStatus = dataStatus == null ? MigrationStepStatusEnum.PENDING.getCode() : dataStatus.getCode();
    }

    /**
     * 当前表在任务配置下是否已全部完成（含跳过）。
     */
    public boolean isTableFinished(boolean enableCopySchema, boolean enableCopyData) {
        if (enableCopySchema && !MigrationStepStatusEnum.isDone(schemaStatus)) {
            return false;
        }
        if (enableCopyData && !MigrationStepStatusEnum.isDone(dataStatus)) {
            return false;
        }
        return enableCopySchema || enableCopyData;
    }
}

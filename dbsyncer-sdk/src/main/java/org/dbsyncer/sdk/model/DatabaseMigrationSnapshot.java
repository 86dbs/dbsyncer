/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.enums.MigrationStepStatusEnum;

import java.io.Serializable;

/**
 * 库级迁移快照（按 {@link DatabaseMapping} 索引）。
 * <p>记录目标库/Schema 创建进度，状态见 {@link MigrationStepStatusEnum}。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 11:30
 */
public class DatabaseMigrationSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 预留游标，与订正校验快照字段对齐 */
    private long cursor;

    /** {@link MigrationStepStatusEnum#getCode()} */
    private int status;

    public DatabaseMigrationSnapshot() {
    }

    public DatabaseMigrationSnapshot(long cursor, int status) {
        this.cursor = cursor;
        this.status = status;
    }

    public DatabaseMigrationSnapshot(long cursor, MigrationStepStatusEnum status) {
        this.cursor = cursor;
        this.status = status == null ? MigrationStepStatusEnum.PENDING.getCode() : status.getCode();
    }

    public long getCursor() {
        return cursor;
    }

    public void setCursor(long cursor) {
        this.cursor = cursor;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void setStatus(MigrationStepStatusEnum status) {
        this.status = status == null ? MigrationStepStatusEnum.PENDING.getCode() : status.getCode();
    }

    public MigrationStepStatusEnum getStatusEnum() {
        return MigrationStepStatusEnum.ofCode(status);
    }
}

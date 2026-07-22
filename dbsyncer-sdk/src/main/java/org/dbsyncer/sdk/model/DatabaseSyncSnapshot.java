/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 库级迁移快照（按 {@link DatabaseMapping} 索引）。
 * <p>库流水线状态见 {@link #status}；其下 {@link #tables} 按表映射 index 记录结构/数据阶段与行级游标。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 11:30
 */
public class DatabaseSyncSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 库映射流水线状态，见 {@link CommonTaskStatusEnum} */
    private int status;

    /** 表级快照：key = 表映射 index（库内唯一） */
    private final ConcurrentHashMap<Integer, DatabaseSyncTableSnapshot> tables = new ConcurrentHashMap<>();

    public DatabaseSyncSnapshot() {
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void setStatus(CommonTaskStatusEnum status) {
        this.status = status == null ? CommonTaskStatusEnum.READY.getCode() : status.getCode();
    }

    public CommonTaskStatusEnum getStatusEnum() {
        return CommonTaskStatusEnum.ofCode(status);
    }

    public ConcurrentHashMap<Integer, DatabaseSyncTableSnapshot> getTables() {
        return tables;
    }

    public DatabaseSyncTableSnapshot getTable(int tableIndex) {
        return tables.get(tableIndex);
    }
}

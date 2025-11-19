package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.connector.sqlserver.cdc.Lsn;

import java.util.List;

public final class CDCEvent {

    private String tableName;
    private int code;
    private List<Object> row;
    private Lsn lsn;  // LSN 信息，用于事件排序

    public CDCEvent(String tableName, int code, List<Object> row, Lsn lsn) {
        this.tableName = tableName;
        this.code = code;
        this.row = row;
        this.lsn = lsn;
    }

    // 保持向后兼容的构造函数
    public CDCEvent(String tableName, int code, List<Object> row) {
        this(tableName, code, row, null);
    }

    public String getTableName() {
        return tableName;
    }

    public int getCode() {
        return code;
    }

    public List<Object> getRow() {
        return row;
    }

    public Lsn getLsn() {
        return lsn;
    }
}
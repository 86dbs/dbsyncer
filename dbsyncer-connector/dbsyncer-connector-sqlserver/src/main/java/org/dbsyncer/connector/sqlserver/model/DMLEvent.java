package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.connector.sqlserver.cdc.Lsn;

import java.util.List;

public final class DMLEvent {

    private String tableName;
    private int code;
    private List<Object> row;
    private Lsn lsn;  // LSN 信息，用于事件排序
    private List<String> columnNames;  // 列名列表，对应 row 数据的顺序（CDC 捕获的列名）

    public DMLEvent(String tableName, int code, List<Object> row, Lsn lsn) {
        this(tableName, code, row, lsn, null);
    }

    public DMLEvent(String tableName, int code, List<Object> row, Lsn lsn, List<String> columnNames) {
        this.tableName = tableName;
        this.code = code;
        this.row = row;
        this.lsn = lsn;
        this.columnNames = columnNames;
    }

    // 保持向后兼容的构造函数
    public DMLEvent(String tableName, int code, List<Object> row) {
        this(tableName, code, row, null, null);
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

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }
}
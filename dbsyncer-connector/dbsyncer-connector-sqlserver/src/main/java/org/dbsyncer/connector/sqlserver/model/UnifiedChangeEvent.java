package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.connector.sqlserver.cdc.Lsn;

import java.util.List;

/**
 * 统一变更事件模型，用于合并 DDL 和 DML 事件
 */
public class UnifiedChangeEvent {
    private Lsn lsn;
    private String eventType;  // "DDL" 或 "DML"
    private String tableName;

    // DDL 相关
    private String ddlCommand;

    // DML 相关
    private DMLEvent DMLEvent;
    
    // DML 事件的列名信息（用于字段映射）
    // 注意：DML 事件中的 row 数据顺序对应 CDC 捕获的列名
    private List<String> columnNames;

    public Lsn getLsn() {
        return lsn;
    }

    public void setLsn(Lsn lsn) {
        this.lsn = lsn;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDdlCommand() {
        return ddlCommand;
    }

    public void setDdlCommand(String ddlCommand) {
        this.ddlCommand = ddlCommand;
    }

    public DMLEvent getCdcEvent() {
        return DMLEvent;
    }

    public void setCdcEvent(DMLEvent DMLEvent) {
        this.DMLEvent = DMLEvent;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }
}


package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.connector.sqlserver.cdc.Lsn;

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
    private CDCEvent cdcEvent;

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

    public CDCEvent getCdcEvent() {
        return cdcEvent;
    }

    public void setCdcEvent(CDCEvent cdcEvent) {
        this.cdcEvent = cdcEvent;
    }
}


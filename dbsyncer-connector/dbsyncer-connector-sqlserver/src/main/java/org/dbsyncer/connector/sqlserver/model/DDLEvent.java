package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.connector.sqlserver.cdc.Lsn;

import java.util.Date;

/**
 * DDL 事件模型
 */
public class DDLEvent {
    private String tableName;
    private String ddlCommand;
    private Lsn ddlLsn;
    private Date ddlTime;

    public DDLEvent(String tableName, String ddlCommand, Lsn ddlLsn, Date ddlTime) {
        this.tableName = tableName;
        this.ddlCommand = ddlCommand;
        this.ddlLsn = ddlLsn;
        this.ddlTime = ddlTime;
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

    public Lsn getDdlLsn() {
        return ddlLsn;
    }

    public void setDdlLsn(Lsn ddlLsn) {
        this.ddlLsn = ddlLsn;
    }

    public Date getDdlTime() {
        return ddlTime;
    }

    public void setDdlTime(Date ddlTime) {
        this.ddlTime = ddlTime;
    }
}


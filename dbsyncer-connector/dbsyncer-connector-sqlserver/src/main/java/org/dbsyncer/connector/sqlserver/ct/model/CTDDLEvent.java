package org.dbsyncer.connector.sqlserver.ct.model;

import java.util.Date;

/**
 * Change Tracking DDL 事件模型
 * 使用 Long 版本号，而不是 Lsn
 */
public class CTDDLEvent {
    private String tableName;
    private String ddlCommand;
    private Long version;  // Change Tracking 版本号（检测时的版本号）
    private Date ddlTime;

    public CTDDLEvent(String tableName, String ddlCommand, Long version, Date ddlTime) {
        this.tableName = tableName;
        this.ddlCommand = ddlCommand;
        this.version = version;
        this.ddlTime = ddlTime;
    }

    // Getters and Setters
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    
    public String getDdlCommand() { return ddlCommand; }
    public void setDdlCommand(String ddlCommand) { this.ddlCommand = ddlCommand; }
    
    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
    
    public Date getDdlTime() { return ddlTime; }
    public void setDdlTime(Date ddlTime) { this.ddlTime = ddlTime; }
}


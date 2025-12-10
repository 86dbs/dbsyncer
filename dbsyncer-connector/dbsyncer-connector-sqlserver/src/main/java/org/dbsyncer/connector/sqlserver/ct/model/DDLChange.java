package org.dbsyncer.connector.sqlserver.ct.model;

/**
 * DDL 变更模型
 */
public class DDLChange {
    private DDLChangeType changeType;
    private String ddlCommand;  // DDL 语句
    private String columnName;  // 列名（主键变更时为 null）

    public DDLChange(DDLChangeType changeType, String ddlCommand, String columnName) {
        this.changeType = changeType;
        this.ddlCommand = ddlCommand;
        this.columnName = columnName;
    }

    // Getters and Setters
    public DDLChangeType getChangeType() { return changeType; }
    public void setChangeType(DDLChangeType changeType) { this.changeType = changeType; }
    
    public String getDdlCommand() { return ddlCommand; }
    public void setDdlCommand(String ddlCommand) { this.ddlCommand = ddlCommand; }
    
    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }
}


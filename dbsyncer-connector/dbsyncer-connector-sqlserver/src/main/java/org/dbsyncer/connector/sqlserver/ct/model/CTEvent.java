package org.dbsyncer.connector.sqlserver.ct.model;

import java.util.List;

/**
 * Change Tracking DML 事件模型
 * 使用 Long 版本号，而不是 Lsn
 */
public class CTEvent {
    private String tableName;
    private String code;  // 操作类型：INSERT, UPDATE, DELETE
    private List<Object> row;  // 行数据
    private Long version;  // Change Tracking 版本号

    public CTEvent(String tableName, String code, List<Object> row, Long version) {
        this.tableName = tableName;
        this.code = code;
        this.row = row;
        this.version = version;
    }

    // Getters and Setters
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    
    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }
    
    public List<Object> getRow() { return row; }
    public void setRow(List<Object> row) { this.row = row; }
    
    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
}


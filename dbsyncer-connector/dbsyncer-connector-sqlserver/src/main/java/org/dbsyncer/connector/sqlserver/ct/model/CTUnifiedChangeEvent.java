package org.dbsyncer.connector.sqlserver.ct.model;

import java.util.Comparator;

/**
 * Change Tracking 统一变更事件模型
 * 用于合并 DDL 和 DML 事件，使用 Long 版本号排序
 */
public class CTUnifiedChangeEvent {
    private Long version;  // Change Tracking 版本号
    private String eventType;  // "DDL" 或 "DML"
    private String tableName;
    
    // DDL 相关
    private String ddlCommand;
    
    // DML 相关
    private CTEvent ctevent;

    public CTUnifiedChangeEvent(String eventType, String tableName, String ddlCommand, 
                                CTEvent ctevent, Long version) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.ddlCommand = ddlCommand;
        this.ctevent = ctevent;
        this.version = version;
    }

    // Getters and Setters
    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
    
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    
    public String getDdlCommand() { return ddlCommand; }
    public void setDdlCommand(String ddlCommand) { this.ddlCommand = ddlCommand; }
    
    public CTEvent getCtevent() { return ctevent; }
    public void setCtevent(CTEvent ctevent) { this.ctevent = ctevent; }
    
    /**
     * 版本号比较器（用于排序）
     */
    public static Comparator<CTUnifiedChangeEvent> versionComparator() {
        return Comparator.comparing(CTUnifiedChangeEvent::getVersion, 
            Comparator.nullsLast(Long::compareTo));
    }
}


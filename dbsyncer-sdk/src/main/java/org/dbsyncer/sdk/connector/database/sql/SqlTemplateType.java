/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql;

/**
 * SQL模板类型枚举
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public enum SqlTemplateType {
    QUERY_STREAM("query_stream", "流式查询"),
    QUERY_CURSOR("query_cursor", "游标查询"),
    QUERY_COUNT("query_count", "计数查询"),
    QUERY_EXIST("query_exist", "存在性检查"),
    INSERT("insert", "插入"),
    UPDATE("update", "更新"),
    DELETE("delete", "删除");
    
    private final String code;
    private final String description;
    
    SqlTemplateType(String code, String description) {
        this.code = code;
        this.description = description;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
}

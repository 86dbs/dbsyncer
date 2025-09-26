/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.context;

import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * SQL构建上下文
 * 包含构建SQL结构所需的所有参数（不包含运行时参数）
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class SqlBuildContext {
    private String schema;
    private String tableName;
    private List<Field> fields;
    private List<String> primaryKeys;
    private String queryFilter;
    private String cursorCondition;
    private String orderByClause;
    
    // 构造方法
    public SqlBuildContext() {}
    
    public SqlBuildContext(String schema, String tableName, List<Field> fields) {
        this.schema = schema;
        this.tableName = tableName;
        this.fields = fields;
    }
    
    // Getter和Setter方法
    public String getSchema() {
        return schema;
    }
    
    public void setSchema(String schema) {
        this.schema = schema;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public List<Field> getFields() {
        return fields;
    }
    
    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
    
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }
    
    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
    
    public String getQueryFilter() {
        return queryFilter;
    }
    
    public void setQueryFilter(String queryFilter) {
        this.queryFilter = queryFilter;
    }
    
    public String getCursorCondition() {
        return cursorCondition;
    }
    
    public void setCursorCondition(String cursorCondition) {
        this.cursorCondition = cursorCondition;
    }
    
    public String getOrderByClause() {
        return orderByClause;
    }
    
    public void setOrderByClause(String orderByClause) {
        this.orderByClause = orderByClause;
    }
}

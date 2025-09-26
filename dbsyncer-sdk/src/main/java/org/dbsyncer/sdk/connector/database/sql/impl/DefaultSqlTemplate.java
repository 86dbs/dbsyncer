/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplateType;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * 默认SQL模板实现
 * 提供适用于大多数数据库的通用SQL模板
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class DefaultSqlTemplate implements SqlTemplate {
    
    @Override
    public String getLeftQuotation() {
        return "\"";
    }
    
    @Override
    public String getRightQuotation() {
        return "\"";
    }
    
    @Override
    public String getQueryTemplate() {
        return "SELECT {fields} FROM {schema}{table} {where}";
    }
    
    @Override
    public String getCountTemplate() {
        return "SELECT COUNT(1) FROM {schema}{table} {where}";
    }
    
    @Override
    public String getInsertTemplate() {
        return "INSERT INTO {schema}{table} ({fields}) VALUES ({placeholders})";
    }
    
    @Override
    public String getUpdateTemplate() {
        return "UPDATE {schema}{table} SET {setClause} WHERE {whereClause}";
    }
    
    @Override
    public String getDeleteTemplate() {
        return "DELETE FROM {schema}{table} WHERE {whereClause}";
    }
    
    @Override
    public String getCursorTemplate() {
        return "{baseQuery} {where} ORDER BY {orderBy} LIMIT ? OFFSET ?";
    }
    
    @Override
    public String getStreamingTemplate() {
        return "{baseQuery} {where}";
    }
    
    @Override
    public String getExistTemplate() {
        return "SELECT 1 FROM {schema}{table} WHERE {whereClause} LIMIT 1";
    }
    
    @Override
    public String buildSql(SqlTemplateType templateType, SqlBuildContext buildContext) {
        String template = getTemplateByType(templateType);
        return processTemplate(template, buildContext);
    }
    
    private String getTemplateByType(SqlTemplateType templateType) {
        switch (templateType) {
            case QUERY_STREAM:
                return getStreamingTemplate();
            case QUERY_CURSOR:
                return getCursorTemplate();
            case QUERY_COUNT:
                return getCountTemplate();
            case QUERY_EXIST:
                return getExistTemplate();
            case INSERT:
                return getInsertTemplate();
            case UPDATE:
                return getUpdateTemplate();
            case DELETE:
                return getDeleteTemplate();
            default:
                throw new IllegalArgumentException("Unsupported template type: " + templateType);
        }
    }
    
    private String processTemplate(String template, SqlBuildContext buildContext) {
        return template
            .replace("{schema}", buildContext.getSchema() != null ? buildContext.getSchema() + "." : "")
            .replace("{table}", buildContext.getTableName())
            .replace("{fields}", buildFieldList(buildContext.getFields()))
            .replace("{where}", buildWhereClause(buildContext))
            .replace("{setClause}", buildSetClause(buildContext))
            .replace("{whereClause}", buildWhereClause(buildContext))
            .replace("{placeholders}", buildPlaceholders(buildContext.getFields().size()))
            .replace("{orderBy}", buildOrderByClause(buildContext.getPrimaryKeys()))
            .replace("{baseQuery}", buildBaseQuery(buildContext));
    }
    
    private String buildFieldList(List<Field> fields) {
        if (fields == null || fields.isEmpty()) {
            return "*";
        }
        
        StringBuilder fieldList = new StringBuilder();
        String quotation = getLeftQuotation();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            fieldList.append(quotation).append(field.getName()).append(quotation);
            if (i < fields.size() - 1) {
                fieldList.append(", ");
            }
        }
        return fieldList.toString();
    }
    
    private String buildWhereClause(SqlBuildContext buildContext) {
        StringBuilder where = new StringBuilder();
        
        if (buildContext.getQueryFilter() != null && !buildContext.getQueryFilter().trim().isEmpty()) {
            where.append(" WHERE ").append(buildContext.getQueryFilter());
        }
        
        if (buildContext.getCursorCondition() != null && !buildContext.getCursorCondition().trim().isEmpty()) {
            if (where.length() > 0) {
                where.append(" AND ").append(buildContext.getCursorCondition());
            } else {
                where.append(" WHERE ").append(buildContext.getCursorCondition());
            }
        }
        
        return where.toString();
    }
    
    private String buildSetClause(SqlBuildContext buildContext) {
        if (buildContext.getFields() == null) {
            return "";
        }
        
        StringBuilder setClause = new StringBuilder();
        String quotation = getLeftQuotation();
        for (int i = 0; i < buildContext.getFields().size(); i++) {
            Field field = buildContext.getFields().get(i);
            if (field.isPk()) {
                continue; // 跳过主键字段
            }
            setClause.append(quotation).append(field.getName()).append(quotation).append(" = ?");
            if (i < buildContext.getFields().size() - 1) {
                setClause.append(", ");
            }
        }
        return setClause.toString();
    }
    
    private String buildPlaceholders(int count) {
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < count; i++) {
            placeholders.append("?");
            if (i < count - 1) {
                placeholders.append(", ");
            }
        }
        return placeholders.toString();
    }
    
    private String buildOrderByClause(List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return "";
        }
        
        StringBuilder orderBy = new StringBuilder();
        String quotation = getLeftQuotation();
        for (int i = 0; i < primaryKeys.size(); i++) {
            orderBy.append(quotation).append(primaryKeys.get(i)).append(quotation);
            if (i < primaryKeys.size() - 1) {
                orderBy.append(", ");
            }
        }
        return orderBy.toString();
    }
    
    private String buildBaseQuery(SqlBuildContext buildContext) {
        return "SELECT " + buildFieldList(buildContext.getFields()) + 
               " FROM " + (buildContext.getSchema() != null ? buildContext.getSchema() + "." : "") + 
               buildContext.getTableName();
    }
}

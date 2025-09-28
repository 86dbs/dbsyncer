/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.List;

/**
 * 默认SQL模板实现
 * 提供适用于大多数数据库的通用SQL模板
 */
public class DefaultSqlTemplate implements SqlTemplate {

    @Override
    public String getLeftQuotation() {
        return "";
    }

    @Override
    public String getRightQuotation() {
        return "";
    }

    @Override
    public String buildQueryStreamSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String fieldList = buildFieldList(buildContext.getFields());
        String queryFilter = buildContext.getQueryFilter();
        return buildQueryStreamSql(schemaTable, fieldList, queryFilter);
    }

    @Override
    public String buildQueryCursorSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String fieldList = buildFieldList(buildContext.getFields());
        String queryFilter = buildContext.getQueryFilter();
        String cursorCondition = buildContext.getCursorCondition();
        List<String> primaryKeys = buildContext.getPrimaryKeys();
        return buildQueryCursorSql(schemaTable, fieldList, queryFilter, cursorCondition, primaryKeys);
    }

    @Override
    public String buildQueryCountSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String queryFilter = buildContext.getQueryFilter();
        return buildQueryCountSql(schemaTable, queryFilter);
    }

    @Override
    public String buildQueryExistSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String queryFilter = buildContext.getQueryFilter();
        return buildQueryExistSql(schemaTable, queryFilter);
    }

    @Override
    public String buildInsertSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        return buildInsertSql(schemaTable, buildContext.getFields());
    }

    @Override
    public String buildUpdateSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        return buildUpdateSql(schemaTable, buildContext.getFields(), buildContext.getPrimaryKeys());
    }

    @Override
    public String buildDeleteSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        return buildDeleteSql(schemaTable, buildContext.getPrimaryKeys());
    }

    protected String buildQueryStreamSql(String schemaTable, String fieldList, String queryFilter) {
        if (StringUtil.isNotBlank(queryFilter)) {
            return String.format("SELECT %s FROM %s %s", fieldList, schemaTable, queryFilter);
        }
        return String.format("SELECT %s FROM %s", fieldList, schemaTable);
    }

    protected String buildQueryCursorSql(String schemaTable, String fieldList, String queryFilter, String cursorCondition, List<String> primaryKeys) {
        String whereClause = "";
        if (StringUtil.isNotBlank(queryFilter) && StringUtil.isNotBlank(cursorCondition)) {
            whereClause = String.format(" %s AND %s", queryFilter, cursorCondition);
        } else if (StringUtil.isNotBlank(queryFilter)) {
            whereClause = " " + queryFilter;
        } else if (StringUtil.isNotBlank(cursorCondition)) {
            whereClause = " WHERE " + cursorCondition;
        }

        String orderByClause = buildOrderByClause(primaryKeys);
        return String.format("SELECT %s FROM %s%s%s", fieldList, schemaTable, whereClause, orderByClause);
    }

    protected String buildQueryCountSql(String schemaTable, String queryFilter) {
        if (StringUtil.isNotBlank(queryFilter)) {
            return String.format("SELECT COUNT(1) FROM %s %s", schemaTable, queryFilter);
        }
        return String.format("SELECT COUNT(1) FROM %s", schemaTable);
    }

    protected String buildQueryExistSql(String schemaTable, String queryFilter) {
        if (StringUtil.isNotBlank(queryFilter)) {
            return String.format("SELECT 1 FROM %s %s LIMIT 1", schemaTable, queryFilter);
        }
        return String.format("SELECT 1 FROM %s LIMIT 1", schemaTable);
    }

    protected String buildInsertSql(String schemaTable, List<Field> fields) {
        String fieldNames = fields.stream()
                .map(field -> buildColumn(field.getName()))
                .collect(java.util.stream.Collectors.joining(", "));
        String placeholders = fields.stream()
                .map(field -> "?")
                .collect(java.util.stream.Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", schemaTable, fieldNames, placeholders);
    }

    protected String buildUpdateSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
        String setClause = fields.stream()
                .filter(field -> !primaryKeys.contains(field.getName()))
                .map(field -> buildColumn(field.getName()) + " = ?")
                .collect(java.util.stream.Collectors.joining(", "));
        String whereClause = primaryKeys.stream()
                .map(pk -> buildColumn(pk) + " = ?")
                .collect(java.util.stream.Collectors.joining(" AND "));
        return String.format("UPDATE %s SET %s WHERE %s", schemaTable, setClause, whereClause);
    }

    protected String buildDeleteSql(String schemaTable, List<String> primaryKeys) {
        String whereClause = primaryKeys.stream()
                .map(pk -> buildColumn(pk) + " = ?")
                .collect(java.util.stream.Collectors.joining(" AND "));
        return String.format("DELETE FROM %s WHERE %s", schemaTable, whereClause);
    }

    /**
     * 创建SQL构建上下文
     * 统一的上下文构建逻辑，所有连接器都可以复用
     *
     * @param commandConfig 命令配置
     * @param buildTableName 构建表名的方法引用
     * @param getQueryFilterSql 获取查询过滤条件的方法引用
     * @return SQL构建上下文
     */
    public SqlBuildContext createBuildContext(CommandConfig commandConfig, 
                                            java.util.function.Function<String, String> buildTableName,
                                            java.util.function.Function<CommandConfig, String> getQueryFilterSql) {
        Table table = commandConfig.getTable();
        DatabaseConfig dbConfig = (DatabaseConfig) commandConfig.getConnectorConfig();
        
        SqlBuildContext buildContext = new SqlBuildContext();
        buildContext.setSchema(buildSchemaWithDot(dbConfig.getSchema()));
        buildContext.setTableName(buildColumn(buildTableName.apply(table.getName())));
        buildContext.setFields(table.getColumn());
        buildContext.setPrimaryKeys(PrimaryKeyUtil.findTablePrimaryKeys(table));
        buildContext.setQueryFilter(getQueryFilterSql.apply(commandConfig));
        buildContext.setCursorCondition(buildCursorConditionFromCached(commandConfig.getCachedPrimaryKeys()));
        
        return buildContext;
    }

    /**
     * 基于缓存的主键列表构建游标条件内容（不包含WHERE关键字）
     * 统一的游标条件构建逻辑
     *
     * @param cachedPrimaryKeys 缓存的主键列表
     * @return 游标条件字符串
     */
    public String buildCursorConditionFromCached(String cachedPrimaryKeys) {
        if (StringUtil.isBlank(cachedPrimaryKeys)) {
            return "";
        }

        // 将 "`id`, `name`, `create_time`" 转换为 "`id` > ? AND `name` > ? AND `create_time` > ?"
        return cachedPrimaryKeys.replaceAll(",", " > ? AND") + " > ?";
    }
}
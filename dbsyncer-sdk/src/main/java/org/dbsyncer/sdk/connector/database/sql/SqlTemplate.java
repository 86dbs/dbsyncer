/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.List;

/**
 * SQL模板接口
 * 负责根据不同的SQL模板类型和构建上下文生成SQL语句
 */
public interface SqlTemplate {

    /**
     * 获取左引号字符
     *
     * @return 左引号字符
     */
    String getLeftQuotation();

    /**
     * 获取右引号字符
     *
     * @return 右引号字符
     */
    String getRightQuotation();

    /**
     * 构建流式查询SQL
     *
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    default String buildQueryStreamSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String fieldList = buildFieldList(buildContext.getFields());
        String queryFilter = buildContext.getQueryFilter();
        List<String> primaryKeys = buildContext.getPrimaryKeys();
        return buildQueryStreamSql(schemaTable, fieldList, queryFilter, primaryKeys);
    }

    /**
     * 构建游标查询SQL
     *
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    default String buildQueryCursorSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String fieldList = buildFieldList(buildContext.getFields());
        String queryFilter = buildContext.getQueryFilter();
        String cursorCondition = buildContext.getCursorCondition();
        List<String> primaryKeys = buildContext.getPrimaryKeys();
        return buildQueryCursorSql(schemaTable, fieldList, queryFilter, cursorCondition, primaryKeys);
    }

    /**
     * 构建计数查询SQL
     *
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    default String buildQueryCountSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String queryFilter = buildContext.getQueryFilter();
        return buildQueryCountSql(schemaTable, queryFilter);
    }

    /**
     * 构建插入SQL
     *
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    default String buildInsertSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        return buildInsertSql(schemaTable, buildContext.getFields());
    }

    /**
     * 构建更新SQL
     *
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    default String buildUpdateSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        return buildUpdateSql(schemaTable, buildContext.getFields(), buildContext.getPrimaryKeys());
    }

    /**
     * 构建删除SQL
     *
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    default String buildDeleteSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        return buildDeleteSql(schemaTable, buildContext.getPrimaryKeys());
    }

    /**
     * 构建覆盖插入SQL（Upsert）
     *
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    default String buildUpsertSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        return buildUpsertSql(schemaTable, buildContext.getFields(), buildContext.getPrimaryKeys());
    }

    /**
     * 构建覆盖插入SQL（Upsert）
     *
     * @param schemaTable 带引号的表名
     * @param fields      字段列表
     * @param primaryKeys 主键列表
     * @return 构建后的SQL（包含?占位符）
     */
    String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys);

    /**
     * 构建DQL查询SQL（在用户SQL基础上添加主键条件）
     *
     * @param userSql     用户原始SQL
     * @param primaryKeys 主键列表
     * @return 构建后的SQL（包含?占位符）
     */
    default String buildDqlQuerySql(String userSql, List<String> primaryKeys) {
        if (userSql == null || userSql.trim().isEmpty()) {
            throw new IllegalArgumentException("User SQL cannot be null or empty");
        }
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return userSql.trim();
        }

        // 清理SQL格式
        String cleanSql = userSql.replace("\t", " ").replace("\r", " ").replace("\n", " ").trim();
        String upperSql = cleanSql.toUpperCase();

        // 检查是否已有WHERE子句
        boolean hasWhere = upperSql.contains(" WHERE ");

        // 构建主键条件
        String pkCondition = buildPrimaryKeyCondition(primaryKeys);

        if (hasWhere) {
            return cleanSql + " AND " + pkCondition;
        } else {
            return cleanSql + " WHERE " + pkCondition;
        }
    }

    /**
     * 构建主键条件
     *
     * @param primaryKeys 主键列表
     * @return 主键条件字符串
     */
    default String buildPrimaryKeyCondition(List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return "";
        }

        return primaryKeys.stream()
                .map(pk -> buildColumn(pk) + " = ?")
                .collect(java.util.stream.Collectors.joining(" AND "));
    }

    /**
     * 构建带引号的表名（用于DDL语句）
     *
     * @param tableName 表名
     * @return 带引号的表名
     */
    default String buildQuotedTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        return getLeftQuotation() + tableName.trim() + getRightQuotation();
    }

    /**
     * 创建SQL构建上下文
     * 统一的上下文构建逻辑，所有连接器都可以复用
     *
     * @param commandConfig     命令配置
     * @param buildTableName    构建表名的方法引用
     * @param getQueryFilterSql 获取查询过滤条件的方法引用
     * @return SQL构建上下文
     */
    default SqlBuildContext createBuildContext(CommandConfig commandConfig,
                                               java.util.function.Function<String, String> buildTableName,
                                               java.util.function.Function<CommandConfig, String> getQueryFilterSql) {
        Table table = commandConfig.getTable();
        DatabaseConfig dbConfig = (DatabaseConfig) commandConfig.getConnectorConfig();

        SqlBuildContext buildContext = new SqlBuildContext();
        buildContext.setSchema(dbConfig.getSchema());
        buildContext.setTableName(buildTableName.apply(table.getName()));
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
    default String buildCursorConditionFromCached(String cachedPrimaryKeys) {
        if (StringUtil.isBlank(cachedPrimaryKeys)) {
            return "";
        }

        // 将 "`id`, `name`, `create_time`" 转换为 "`id` > ? AND `name` > ? AND `create_time` > ?"
        return cachedPrimaryKeys.replaceAll(",", " > ? AND") + " > ?";
    }

    /**
     * 构建带引号的字符串列表（通用方法）
     * 统一的引号处理逻辑，所有需要引号的地方都可以使用
     *
     * @param items 字符串列表
     * @return 带引号的字符串，用逗号分隔
     */
    default String buildQuotedStringList(List<String> items) {
        if (items == null || items.isEmpty()) {
            return "";
        }

        return items.stream()
                .map(this::buildColumn)
                .collect(java.util.stream.Collectors.joining(", "));
    }

    // Helper methods for building SQL parts (from DefaultSqlTemplate)
    default String buildQueryStreamSql(String schemaTable, String fieldList, String queryFilter, List<String> primaryKeys) {
        String orderByClause = buildOrderByClause(primaryKeys);
        if (StringUtil.isNotBlank(queryFilter)) {
            return String.format("SELECT %s FROM %s %s%s", fieldList, schemaTable, queryFilter, orderByClause);
        }
        return String.format("SELECT %s FROM %s%s", fieldList, schemaTable, orderByClause);
    }

    default String buildQueryCursorSql(String schemaTable, String fieldList, String queryFilter, String cursorCondition, List<String> primaryKeys) {
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

    default String buildQueryCountSql(String schemaTable, String queryFilter) {
        if (StringUtil.isNotBlank(queryFilter)) {
            return String.format("SELECT COUNT(1) FROM %s %s", schemaTable, queryFilter);
        }
        return String.format("SELECT COUNT(1) FROM %s", schemaTable);
    }

    default String buildInsertSql(String schemaTable, List<Field> fields) {
        String fieldNames = fields.stream()
                .map(field -> buildColumn(field.getName()))
                .collect(java.util.stream.Collectors.joining(", "));
        String placeholders = fields.stream()
                .map(field -> "?")
                .collect(java.util.stream.Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", schemaTable, fieldNames, placeholders);
    }

    default String buildUpdateSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
        String setClause = fields.stream()
                .filter(field -> !field.isPk())
                .map(field -> buildColumn(field.getName()) + " = ?")
                .collect(java.util.stream.Collectors.joining(", "));
        String whereClause = primaryKeys.stream()
                .map(pk -> buildColumn(pk) + " = ?")
                .collect(java.util.stream.Collectors.joining(" AND "));
        return String.format("UPDATE %s SET %s WHERE %s", schemaTable, setClause, whereClause);
    }

    default String buildDeleteSql(String schemaTable, List<String> primaryKeys) {
        String whereClause = primaryKeys.stream()
                .map(pk -> buildColumn(pk) + " = ?")
                .collect(java.util.stream.Collectors.joining(" AND "));
        return String.format("DELETE FROM %s WHERE %s", schemaTable, whereClause);
    }

    // Helper methods for building common SQL parts
    default String buildTable(String schema, String tableName) {
        if (schema != null && !schema.isEmpty()) {
            return getLeftQuotation() + schema + getRightQuotation() + "." + getLeftQuotation() + tableName + getRightQuotation();
        }
        return getLeftQuotation() + tableName + getRightQuotation();
    }

    default String buildColumn(String columnName) {
        return getLeftQuotation() + columnName + getRightQuotation();
    }

    default String buildFieldList(List<Field> fields) {
        if (fields == null || fields.isEmpty()) {
            return "*";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            // "USER_NAME" as "myName"
            Field field = fields.get(i);
            sb.append(buildColumn(field.getName()));
            if (StringUtil.isNotBlank(field.getLabelName())) {
                sb.append(" as ").append(getLeftQuotation()).append(field.getLabelName()).append(getRightQuotation());
            }
            if (i < fields.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    default String buildOrderByClause(List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" ORDER BY ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            sb.append(buildColumn(primaryKeys.get(i)));
            if (i < primaryKeys.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * 构建带引号的schema名称
     *
     * @param schema schema名称
     * @return 带引号的schema名称，如果schema为空则返回空字符串
     */
    default String buildSchema(String schema) {
        if (schema == null || schema.isEmpty()) {
            return "";
        }
        return getLeftQuotation() + schema + getRightQuotation();
    }

    /**
     * 构建带引号的字段名称列表
     *
     * @param fieldNames 字段名称列表
     * @param separator  分隔符
     * @return 带引号的字段名称列表
     */
    default String buildQuotedFieldList(List<String> fieldNames, String separator) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return "";
        }
        return fieldNames.stream()
                .map(this::buildColumn)
                .collect(java.util.stream.Collectors.joining(separator));
    }

    /**
     * 构建带引号的字段名称列表（默认用逗号分隔）
     *
     * @param fieldNames 字段名称列表
     * @return 带引号的字段名称列表
     */
    default String buildQuotedFieldList(List<String> fieldNames) {
        return buildQuotedFieldList(fieldNames, ", ");
    }

    default String buildBatchInsertSql(String schemaTable, List<Field> fields, int rowCount){
        return "";
    }

    default String buildBatchUpsertSql(String schemaTable, List<Field> fields, int rowCount, List<String> primaryKeys){
        return "";
    }
}
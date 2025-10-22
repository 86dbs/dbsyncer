/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * SQL Server特定SQL模板实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class SqlServerTemplate implements SqlTemplate {

    @Override
    public String getLeftQuotation() {
        return "[";
    }

    @Override
    public String getRightQuotation() {
        return "]";
    }

    @Override
    public String buildQueryStreamSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String fieldList = buildFieldList(buildContext.getFields());
        String queryFilter = buildContext.getQueryFilter();
        String orderByClause = buildOrderByClause(buildContext.getPrimaryKeys());


        if (StringUtil.isNotBlank(queryFilter)) {
            return String.format("SELECT %s FROM %s WITH (NOLOCK) %s%s", fieldList, schemaTable, queryFilter,orderByClause);
        }
        return String.format("SELECT %s FROM %s WITH (NOLOCK)%s", fieldList, schemaTable, orderByClause);
    }

    @Override
    public String buildQueryCursorSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String fieldList = buildFieldList(buildContext.getFields());
        String queryFilter = buildContext.getQueryFilter();
        String cursorCondition = buildContext.getCursorCondition();
        List<String> primaryKeys = buildContext.getPrimaryKeys();

        String whereClause = "";
        if (StringUtil.isNotBlank(queryFilter) && StringUtil.isNotBlank(cursorCondition)) {
            whereClause = String.format(" %s AND %s", queryFilter, cursorCondition);
        } else if (StringUtil.isNotBlank(queryFilter)) {
            whereClause = " " + queryFilter;
        } else if (StringUtil.isNotBlank(cursorCondition)) {
            whereClause = " WHERE " + cursorCondition;
        }

        String orderByClause = buildOrderByClause(primaryKeys);
        return String.format("SELECT %s FROM %s WITH (NOLOCK)%s%s", fieldList, schemaTable, whereClause, orderByClause);
    }

    @Override
    public String buildQueryCountSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String queryFilter = buildContext.getQueryFilter();

        if (StringUtil.isNotBlank(queryFilter)) {
            return String.format("SELECT COUNT(*) FROM %s WITH (NOLOCK) %s", schemaTable, queryFilter);
        }
        return String.format("SELECT COUNT(*) FROM %s WITH (NOLOCK)", schemaTable);
    }

    /**
     * 构建SQL Server批量插入SQL
     *
     * @param schemaTable 带schema的表名
     * @param fields      字段列表
     * @param rowCount    行数
     * @return 批量插入SQL
     */
    @Override
    public String buildBatchInsertSql(String schemaTable, List<Field> fields, int rowCount) {
        // 构建列名列表
        String columnList = buildQuotedFieldList(fields.stream()
                .map(Field::getName)
                .collect(java.util.stream.Collectors.toList()));

        // 构建单行参数模板
        String rowTemplate = "(" + fields.stream()
                .map(field -> "?")
                .collect(java.util.stream.Collectors.joining(", ")) + ")";

        // 构建多行 VALUES 子句
        String valuesClause = java.util.stream.IntStream.range(0, rowCount)
                .mapToObj(i -> rowTemplate)
                .collect(java.util.stream.Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES %s", schemaTable, columnList, valuesClause);
    }

    /**
     * 构建SQL Server批量UPSERT SQL (MERGE语句)
     *
     * @param schemaTable 带schema的表名
     * @param fields      字段列表
     * @param rowCount    行数
     * @param primaryKeys 主键列表
     * @return 批量UPSERT SQL
     */
    @Override
    public String buildBatchUpsertSql(String schemaTable, List<Field> fields, int rowCount, List<String> primaryKeys) {
        // 构建列名列表
        String columnList = buildQuotedFieldList(fields.stream()
                .map(Field::getName)
                .collect(java.util.stream.Collectors.toList()));

        // 构建单行参数模板
        String rowTemplate = "(" + fields.stream()
                .map(field -> "?")
                .collect(java.util.stream.Collectors.joining(", ")) + ")";

        // 构建多行 VALUES 子句
        String valuesClause = java.util.stream.IntStream.range(0, rowCount)
                .mapToObj(i -> rowTemplate)
                .collect(java.util.stream.Collectors.joining(", "));

        // 构建 ON 条件（主键匹配）
        String onCondition = primaryKeys.stream()
                .map(key -> "target." + buildColumn(key) + " = source." + buildColumn(key))
                .collect(java.util.stream.Collectors.joining(" AND "));

        // 构建 UPDATE SET 子句（非主键字段）
        String updateClause = fields.stream()
                .filter(field -> !primaryKeys.contains(field.getName()))
                .map(field -> "target." + buildColumn(field.getName()) + " = source." + buildColumn(field.getName()))
                .collect(java.util.stream.Collectors.joining(", "));

        // 构建 INSERT VALUES 子句
        String insertValues = fields.stream()
                .map(field -> "source." + buildColumn(field.getName()))
                .collect(java.util.stream.Collectors.joining(", "));

        // 组合完整的 MERGE SQL
        return String.format(
                "MERGE %s AS target " +
                        "USING (VALUES %s) AS source (%s) " +
                        "ON (%s) " +
                        "WHEN MATCHED THEN UPDATE SET %s " +
                        "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
                schemaTable, valuesClause, columnList, onCondition, updateClause, columnList, insertValues
        );
    }

    /**
     * 构建SQL Server IDENTITY_INSERT开关SQL
     *
     * @param schemaTable 带schema的表名
     * @param enable      是否启用
     * @return IDENTITY_INSERT SQL
     */
    public String buildIdentityInsertSql(String schemaTable, boolean enable) {
        String action = enable ? "ON" : "OFF";
        return String.format("SET IDENTITY_INSERT %s %s", schemaTable, action);
    }
}

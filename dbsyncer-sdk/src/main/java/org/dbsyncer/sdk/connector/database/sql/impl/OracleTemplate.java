/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;

import org.dbsyncer.sdk.model.Field;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Oracle特定SQL模板实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class OracleTemplate implements SqlTemplate {

    @Override
    public String getLeftQuotation() {
        return "\"";
    }

    @Override
    public String getRightQuotation() {
        return "\"";
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
        String innerQuery = String.format("SELECT %s FROM %s%s%s", fieldList, schemaTable, whereClause, orderByClause);
        return String.format("SELECT * FROM (SELECT A.*, ROWNUM RN FROM (%s)A WHERE ROWNUM <= ?) WHERE RN > ?", innerQuery);
    }
    
    @Override
    public String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
        String fieldNames = fields.stream()
                .map(field -> buildColumn(field.getName()))
                .collect(Collectors.joining(", "));
        String placeholders = fields.stream()
                .map(field -> "?")
                .collect(Collectors.joining(", "));
        
        // 构建MERGE INTO语句
        String updateClause = fields.stream()
                .filter(field -> !field.isPk())
                .map(field -> buildColumn(field.getName()) + " = SRC." + buildColumn(field.getName()))
                .collect(Collectors.joining(", "));
        
        String insertClause = fields.stream()
                .map(field -> "SRC." + buildColumn(field.getName()))
                .collect(Collectors.joining(", "));
        
        String pkCondition = primaryKeys.stream()
                .map(pk -> "TGT." + buildColumn(pk) + " = SRC." + buildColumn(pk))
                .collect(Collectors.joining(" AND "));
        
        String pkFields = primaryKeys.stream()
                .map(this::buildColumn)
                .collect(Collectors.joining(", "));
        
        return String.format(
            "MERGE INTO %s TGT " +
            "USING (SELECT %s FROM DUAL) SRC ON (%s) " +
            "WHEN MATCHED THEN UPDATE SET %s " +
            "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
            schemaTable, placeholders, pkCondition, updateClause, pkFields, insertClause);
    }
}

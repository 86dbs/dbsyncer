/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Oracle特定SQL模板实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class OracleTemplate extends AbstractSqlTemplate {

    public OracleTemplate(SchemaResolver schemaResolver) {
        super(schemaResolver);
    }

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

    @Override
    public String buildAddColumnSql(String tableName, Field field) {
        return String.format("ALTER TABLE %s ADD (%s %s)",
                buildQuotedTableName(tableName),
                buildColumn(field.getName()),
                convertToDatabaseType(field));
    }

    @Override
    public String buildModifyColumnSql(String tableName, Field field) {
        return String.format("ALTER TABLE %s MODIFY (%s %s)",
                buildQuotedTableName(tableName),
                buildColumn(field.getName()),
                convertToDatabaseType(field));
    }

    @Override
    public String buildRenameColumnSql(String tableName, String oldFieldName, Field newField) {
        return String.format("ALTER TABLE %s RENAME COLUMN %s TO %s",
                buildQuotedTableName(tableName),
                buildColumn(oldFieldName),
                buildColumn(newField.getName()));
    }

    @Override
    public String buildDropColumnSql(String tableName, String fieldName) {
        return String.format("ALTER TABLE %s DROP COLUMN %s",
                buildQuotedTableName(tableName),
                buildColumn(fieldName));
    }

    @Override
    public String convertToDatabaseType(Field column) {
        // 使用SchemaResolver进行类型转换，完全委托给SchemaResolver处理
        Field targetField = schemaResolver.fromStandardType(column);
        String typeName = targetField.getTypeName();
        
        // 处理参数（长度、精度等）
        switch (typeName.toUpperCase()) {
            case "VARCHAR2":
            case "NVARCHAR2":
                if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                throw new IllegalArgumentException("should give size for column: " + column.getTypeName());
            case "NUMBER":
                if (column.getColumnSize() > 0 && column.getRatio() >= 0) {
                    return typeName + "(" + column.getColumnSize() + "," + column.getRatio() + ")";
                } else if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                return "NUMBER(10,0)";
            default:
                return typeName;
        }
    }
}
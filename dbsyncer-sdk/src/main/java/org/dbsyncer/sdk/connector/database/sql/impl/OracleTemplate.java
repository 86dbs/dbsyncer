/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.ArrayList;
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
        
        // 如果所有字段都是主键，updateClause 为空，需要至少添加一个更新表达式以满足 Oracle 语法要求
        // 使用第一个主键字段的虚拟更新（不会实际改变值）
        if (updateClause.isEmpty() && !fields.isEmpty()) {
            Field firstField = fields.get(0);
            updateClause = buildColumn(firstField.getName()) + " = SRC." + buildColumn(firstField.getName());
        }

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
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" ADD (").append(buildColumn(field.getName()))
           .append(" ").append(convertToDatabaseType(field));
        
        // 添加 NOT NULL 约束
        if (field.getNullable() != null && !field.getNullable()) {
            sql.append(" NOT NULL");
        }
        
        // 注意：不再支持 DEFAULT 值，因为数据同步不需要默认值支持
        
        sql.append(")");
        
        // Oracle 的注释需要使用 COMMENT ON COLUMN，这里暂时不处理
        // 如果需要添加注释，可以使用：
        // COMMENT ON COLUMN tableName.fieldName IS 'comment';
        
        return sql.toString();
    }

    @Override
    public String buildModifyColumnSql(String tableName, Field field) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" MODIFY (").append(buildColumn(field.getName()))
           .append(" ").append(convertToDatabaseType(field));
        
        // 添加 NOT NULL 约束
        if (field.getNullable() != null && !field.getNullable()) {
            sql.append(" NOT NULL");
        }
        
        // 注意：不再支持 DEFAULT 值，因为数据同步不需要默认值支持
        
        sql.append(")");
        
        return sql.toString();
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
    public String buildCreateTableSql(String schema, String tableName, List<Field> fields, List<String> primaryKeys) {
        List<String> columnDefs = new ArrayList<>();
        for (Field field : fields) {
            String ddlType = convertToDatabaseType(field);
            String columnName = buildColumn(field.getName());
            
            // 构建列定义：列名 类型 [NOT NULL]
            // 注意：不再支持 DEFAULT 值，因为数据同步不需要默认值支持
            // 注意：Oracle 的自增字段需要使用序列和触发器，这里暂时不处理
            // 如果需要自增，可以使用：GENERATED ALWAYS AS IDENTITY 或序列+触发器
            StringBuilder columnDef = new StringBuilder();
            columnDef.append(String.format("  %s %s", columnName, ddlType));
            
            if (field.getNullable() != null && !field.getNullable()) {
                columnDef.append(" NOT NULL");
            }
            
            columnDefs.add(columnDef.toString());
        }
        
        // 构建主键定义
        String pkClause = "";
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            String pkColumns = primaryKeys.stream()
                    .map(this::buildColumn)
                    .collect(java.util.stream.Collectors.joining(", "));
            pkClause = String.format(",\n  PRIMARY KEY (%s)", pkColumns);
        }
        
        // 组装完整的 CREATE TABLE 语句
        String columns = String.join(",\n", columnDefs);
        return String.format("CREATE TABLE %s (\n%s%s\n)",
                buildTable(schema, tableName), columns, pkClause);
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

    @Override
    public String buildMetadataCountSql(String schema, String tableName) {
        // 转义单引号防止SQL注入
        String escapedTableName = tableName.replace("'", "''");
        
        if (schema != null && !schema.trim().isEmpty()) {
            String escapedSchema = schema.replace("'", "''");
            return String.format(
                "SELECT num_rows FROM all_tables WHERE owner = UPPER('%s') AND table_name = UPPER('%s')",
                escapedSchema,
                escapedTableName
            );
        } else {
            // 使用 user_tables（当前用户的表）
            return String.format(
                "SELECT num_rows FROM user_tables WHERE table_name = UPPER('%s')",
                escapedTableName
            );
        }
    }
}
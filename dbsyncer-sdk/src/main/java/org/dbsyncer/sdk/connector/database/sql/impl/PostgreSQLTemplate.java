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
 * PostgreSQL特定SQL模板实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class PostgreSQLTemplate extends AbstractSqlTemplate {
    
    public PostgreSQLTemplate(SchemaResolver schemaResolver) {
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
        return String.format("SELECT %s FROM %s%s%s LIMIT ? OFFSET ?", fieldList, schemaTable, whereClause, orderByClause);
    }
    
    @Override
    public String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
        String fieldNames = fields.stream()
                .map(field -> buildColumn(field.getName()))
                .collect(Collectors.joining(", "));
        String placeholders = fields.stream()
                .map(field -> "?")
                .collect(Collectors.joining(", "));
        String updateClause = fields.stream()
                .filter(field -> !field.isPk())
                .map(field -> buildColumn(field.getName()) + " = EXCLUDED." + buildColumn(field.getName()))
                .collect(Collectors.joining(", "));
        
        // 如果所有字段都是主键，updateClause 为空，需要至少添加一个更新表达式以满足 PostgreSQL 语法要求
        // 使用第一个主键字段的虚拟更新（不会实际改变值）
        if (updateClause.isEmpty() && !fields.isEmpty()) {
            Field firstField = fields.get(0);
            updateClause = buildColumn(firstField.getName()) + " = EXCLUDED." + buildColumn(firstField.getName());
        }
        
        String conflictClause = primaryKeys.stream()
                .map(this::buildColumn)
                .collect(Collectors.joining(", "));
        
        return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", 
                             schemaTable, fieldNames, placeholders, conflictClause, updateClause);
    }
    
    @Override
    public String buildAddColumnSql(String tableName, Field field) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" ADD COLUMN ").append(buildColumn(field.getName()))
           .append(" ").append(convertToDatabaseType(field));
        
        // 添加 NOT NULL 约束
        if (field.getNullable() != null && !field.getNullable()) {
            sql.append(" NOT NULL");
        }
        
        // 添加 DEFAULT 值
        if (field.getDefaultValue() != null && !field.getDefaultValue().isEmpty()) {
            sql.append(" DEFAULT ").append(field.getDefaultValue());
        }
        
        // PostgreSQL 的注释需要使用 COMMENT ON COLUMN，这里暂时不处理
        // 如果需要添加注释，可以使用：
        // COMMENT ON COLUMN tableName.fieldName IS 'comment';
        
        return sql.toString();
    }

    @Override
    public String buildModifyColumnSql(String tableName, Field field) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" ALTER COLUMN ").append(buildColumn(field.getName()))
           .append(" TYPE ").append(convertToDatabaseType(field));
        
        // PostgreSQL 的 ALTER COLUMN TYPE 和设置约束需要分开执行
        // 这里只处理类型修改，NOT NULL 和 DEFAULT 需要单独的语句
        // 如果需要完整的修改，应该生成多个 ALTER TABLE 语句
        
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
    public String convertToDatabaseType(Field column) {
        // 使用SchemaResolver进行类型转换，完全委托给SchemaResolver处理
        Field targetField = schemaResolver.fromStandardType(column);
        String typeName = targetField.getTypeName();
        
        // 处理参数（长度、精度等）
        switch (typeName) {
            case "VARCHAR":
                if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                throw new IllegalArgumentException("should give size for column: " + column.getTypeName());
            case "DECIMAL":
                if (column.getColumnSize() > 0 && column.getRatio() >= 0) {
                    return typeName + "(" + column.getColumnSize() + "," + column.getRatio() + ")";
                } else if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                return "DECIMAL(10,0)";
            default:
                return typeName;
        }
    }

}
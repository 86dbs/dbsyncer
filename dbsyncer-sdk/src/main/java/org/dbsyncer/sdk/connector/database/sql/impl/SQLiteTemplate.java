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
 * SQLite特定SQL模板实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class SQLiteTemplate extends AbstractSqlTemplate {

    public SQLiteTemplate(SchemaResolver schemaResolver) {
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

        // SQLite使用INSERT OR REPLACE语法
        return String.format("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
                schemaTable, fieldNames, placeholders);
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
        
        // 注意：不再支持 DEFAULT 值，因为数据同步不需要默认值支持
        
        // SQLite 不支持列注释
        
        return sql.toString();
    }

    @Override
    public String buildModifyColumnSql(String tableName, Field field) {
        // SQLite不支持直接修改列，需要重建表
        return "";
    }

    @Override
    public String buildRenameColumnSql(String tableName, String oldFieldName, Field newField) {
        // SQLite不支持直接重命名列，需要重建表
        return "";
    }

    @Override
    public String buildDropColumnSql(String tableName, String fieldName) {
        // SQLite不支持直接删除列，需要重建表
        return "";
    }

    @Override
    public String convertToDatabaseType(Field column) {
        // 使用SchemaResolver进行类型转换，完全委托给SchemaResolver处理
        Field targetField = schemaResolver.fromStandardType(column);
        String typeName = targetField.getTypeName();
        
        // 处理参数（长度、精度等）
        switch (typeName) {
            case "TEXT":
                if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                // SQLite的TEXT类型实际上不需要指定长度，但为了保持一致性，我们还是处理这个情况
                return typeName;
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
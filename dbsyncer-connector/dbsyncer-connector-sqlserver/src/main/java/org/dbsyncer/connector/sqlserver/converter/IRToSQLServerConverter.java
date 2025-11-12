/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.converter;

import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.converter.IRToTargetConverter;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;

import java.util.List;
import java.util.Map;

/**
 * 中间表示到SQL Server转换器
 */
public class IRToSQLServerConverter implements IRToTargetConverter {

    private final SqlTemplate sqlServerTemplate;

    public IRToSQLServerConverter(SqlTemplate sqlServerTemplate) {
        // 构造函数，可以传入带有SchemaResolver的SqlServerTemplate
        this.sqlServerTemplate = sqlServerTemplate;
    }

    @Override
    public String convert(DDLIntermediateRepresentation ir) {
        if (ir == null) {
            throw new IllegalArgumentException("DDLIntermediateRepresentation cannot be null");
        }
        
        if (ir.getOperationType() == null) {
            throw new IllegalArgumentException("AlterOperation cannot be null in DDLIntermediateRepresentation");
        }
        
        StringBuilder sql = new StringBuilder();

        switch (ir.getOperationType()) {
            case ADD:
                sql.append(convertColumnsToAdd(ir.getTableName(), ir.getColumns()));
                break;
            case MODIFY:
                sql.append(convertColumnsToModify(ir.getTableName(), ir.getColumns()));
                break;
            case CHANGE:
                sql.append(convertColumnsToChange(ir.getTableName(), ir.getColumns(), ir.getOldToNewColumnNames()));
                break;
            case DROP:
                sql.append(convertColumnsToDrop(ir.getTableName(), ir.getColumns()));
                break;
            default:
                throw new IllegalArgumentException("Unsupported AlterOperation: " + ir.getOperationType());
        }
        return sql.toString();
    }

    private String convertColumnsToAdd(String tableName, List<Field> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        
        // SQL Server支持在单个ALTER TABLE语句中添加多个列
        // 格式: ALTER TABLE [table] ADD [col1] type1, [col2] type2
        StringBuilder result = new StringBuilder();
        String quotedTableName = sqlServerTemplate.buildQuotedTableName(tableName);
        result.append("ALTER TABLE ").append(quotedTableName).append(" ADD ");
        
        // 从buildAddColumnSql的结果中提取列定义部分
        // buildAddColumnSql返回: "ALTER TABLE [table] ADD [col] type"
        // 我们需要提取 "[col] type" 部分
        String prefix = "ALTER TABLE " + quotedTableName + " ADD ";
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            String fullSql = sqlServerTemplate.buildAddColumnSql(tableName, column);
            // 提取列定义部分（去掉 "ALTER TABLE [table] ADD " 前缀）
            String columnDef = fullSql.substring(prefix.length());
            result.append(columnDef);
        }
        return result.toString();
    }

    private String convertColumnsToModify(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append(sqlServerTemplate.buildModifyColumnSql(tableName, column));
        }
        return result.toString();
    }

    private String convertColumnsToChange(String tableName, List<Field> columns, Map<String, String> oldToNewColumnNames) {
        StringBuilder result = new StringBuilder();
        // SQL Server使用sp_rename存储过程进行重命名
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append("; ");
            }
            Field column = columns.get(i);
            String newColumnName = column.getName();
            // 从映射中获取旧字段名
            String oldColumnName = null;
            for (Map.Entry<String, String> entry : oldToNewColumnNames.entrySet()) {
                if (entry.getValue().equals(newColumnName)) {
                    oldColumnName = entry.getKey();
                    break;
                }
            }
            // 如果找不到旧字段名，说明字段名没有改变，只改变了类型，使用MODIFY操作
            if (oldColumnName == null || oldColumnName.equals(newColumnName)) {
                // 字段名没有改变，只改变类型，使用ALTER COLUMN
                result.append(sqlServerTemplate.buildModifyColumnSql(tableName, column));
            } else {
                // 字段名改变了，需要两步操作：
                // 1. 先使用sp_rename重命名字段
                result.append(sqlServerTemplate.buildRenameColumnSql(tableName, oldColumnName, column));
                // 2. 然后使用ALTER COLUMN修改类型（CHANGE操作总是包含新的类型定义）
                result.append("; ");
                result.append(sqlServerTemplate.buildModifyColumnSql(tableName, column));
            }
        }
        return result.toString();
    }

    private String convertColumnsToDrop(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append(sqlServerTemplate.buildDropColumnSql(tableName, column.getName()));
        }
        return result.toString();
    }
}
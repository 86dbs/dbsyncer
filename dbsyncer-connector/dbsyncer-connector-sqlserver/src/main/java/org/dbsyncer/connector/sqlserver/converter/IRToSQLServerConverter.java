package org.dbsyncer.connector.sqlserver.converter;

import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.converter.AbstractIRToTargetConverter;

import java.util.List;
import java.util.Map;

/**
 * 中间表示到SQL Server转换器
 */
public class IRToSQLServerConverter extends AbstractIRToTargetConverter {

    private final SqlTemplate sqlServerTemplate;

    public IRToSQLServerConverter(SqlTemplate sqlServerTemplate) {
        // 构造函数，可以传入带有SchemaResolver的SqlServerTemplate
        this.sqlServerTemplate = sqlServerTemplate;
    }

    @Override
    protected String convertOperation(String tableName, String schema, AlterOperation operation, List<Field> columns, 
                                      Map<String, String> oldToNewColumnNames) {
        switch (operation) {
            case ADD:
                return convertColumnsToAdd(tableName, schema, columns);
            case MODIFY:
                return convertColumnsToModify(tableName, schema, columns);
            case CHANGE:
                return convertColumnsToChange(tableName, schema, columns, oldToNewColumnNames);
            case DROP:
                return convertColumnsToDrop(tableName, columns);
            default:
                throw new IllegalArgumentException("Unsupported AlterOperation: " + operation);
        }
    }

    private String convertColumnsToAdd(String tableName, String schema, List<Field> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        
        // SQL Server支持在单个ALTER TABLE语句中添加多个列
        // 格式: ALTER TABLE [table] ADD [col1] type1, [col2] type2
        StringBuilder result = new StringBuilder();
        String quotedTableName = sqlServerTemplate.buildQuotedTableName(tableName);
        
        // 收集需要添加 COMMENT 的列
        java.util.List<Field> columnsWithComment = new java.util.ArrayList<>();
        
        // 直接构建列定义，避免字符串提取可能的问题
        org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate sqlServerTemplateImpl = 
            (org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate) sqlServerTemplate;
        
        // 收集要添加的列定义
        java.util.List<String> columnDefinitions = new java.util.ArrayList<>();
        
        for (int i = 0; i < columns.size(); i++) {
            Field column = columns.get(i);
            
            // 直接构建列定义：[col] type [NOT NULL] [DEFAULT value]
            StringBuilder columnDef = new StringBuilder();
            String databaseType = sqlServerTemplateImpl.convertToDatabaseType(column);
            columnDef.append(sqlServerTemplate.buildColumn(column.getName()))
                     .append(" ").append(databaseType);
            
            // 添加 NOT NULL 约束
            if (column.getNullable() != null && !column.getNullable()) {
                columnDef.append(" NOT NULL");
                // SQL Server 语法要求：向非空表添加 NOT NULL 列时，必须提供 DEFAULT 值
                // 注意：这是为了满足 SQL Server 的语法约束，不是通用的缺省值处理
                // 生成的 DEFAULT 值仅用于满足语法要求，不会影响数据同步结果
                // 使用转换后的 SQL Server 类型名称（databaseType，如 "BIGINT"）来判断默认值
                String defaultValue = SqlServerTemplate.getDefaultValueForNotNullColumnByTypeName(databaseType);
                if (defaultValue != null) {
                    columnDef.append(" DEFAULT ").append(defaultValue);
                }
            }
            
            columnDefinitions.add(columnDef.toString());
            
            // 收集有 COMMENT 的列
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                columnsWithComment.add(column);
            }
        }
        
        // 如果有要添加的列，生成单个 ALTER TABLE 语句（SQL Server支持在单个语句中添加多个列）
        // 移除 IF NOT EXISTS 检查，因为：
        // 1. DDL 同步场景中，如果列已存在，说明可能已经同步过了，应该报错而不是静默跳过
        // 2. 合并多个列到一个 ALTER TABLE 语句中，减少分号的使用，避免 split 的需求
        if (!columnDefinitions.isEmpty()) {
            result.append("ALTER TABLE ").append(quotedTableName).append(" ADD ");
            for (int i = 0; i < columnDefinitions.size(); i++) {
                if (i > 0) {
                    result.append(", ");
                }
                result.append(columnDefinitions.get(i));
            }
        }
        
        // 如果有 COMMENT，追加 COMMENT 语句（使用分号分隔，作为独立的 SQL 语句）
        // 注意：COMMENT 必须使用独立的 EXEC 语句，无法合并到 ALTER TABLE 中
        if (!columnsWithComment.isEmpty()) {
            String effectiveSchema = (schema != null && !schema.trim().isEmpty()) ? schema : "dbo";
            for (Field column : columnsWithComment) {
                result.append("; ");
                result.append(sqlServerTemplateImpl
                    .buildCommentSql(effectiveSchema, tableName, column.getName(), column.getComment()));
            }
        }
        
        return result.toString();
    }

    private String convertColumnsToModify(String tableName, String schema, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append("; ");
            }
            Field column = columns.get(i);
            // 1. 生成 ALTER COLUMN 语句
            result.append(sqlServerTemplate.buildModifyColumnSql(tableName, column));
            
            // 2. 注意：不再支持 DEFAULT 值，因为数据同步不需要默认值支持
            
            // 3. 如果有 COMMENT，添加 COMMENT 语句
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                result.append("; ");
                String effectiveSchema = (schema != null && !schema.trim().isEmpty()) ? schema : "dbo";
                result.append(((org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate) sqlServerTemplate)
                    .buildCommentSql(effectiveSchema, tableName, column.getName(), column.getComment()));
            }
        }
        return result.toString();
    }

    private String convertColumnsToChange(String tableName, String schema, List<Field> columns, Map<String, String> oldToNewColumnNames) {
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
                // 字段名没有改变，只改变类型，使用ALTER COLUMN（包含 DEFAULT 和 COMMENT 处理）
                String modifySql = convertColumnsToModify(tableName, schema, java.util.Collections.singletonList(column));
                result.append(modifySql);
            } else {
                // 字段名改变了，需要两步操作：
                // 1. 先使用sp_rename重命名字段
                // sp_rename 需要完整的对象名称，使用方括号避免特殊字符问题：'[schema].[table].[column]'
                String effectiveSchema = (schema != null && !schema.trim().isEmpty()) ? schema : "dbo";
                String fullObjectName = String.format("[%s].[%s].[%s]", effectiveSchema, tableName, oldColumnName);
                result.append(String.format("EXEC sp_rename '%s', '%s', 'COLUMN'",
                        fullObjectName, newColumnName));
                // 2. 然后使用ALTER COLUMN修改类型（CHANGE操作总是包含新的类型定义，包含 DEFAULT 和 COMMENT 处理）
                result.append("; ");
                String modifySql = convertColumnsToModify(tableName, schema, java.util.Collections.singletonList(column));
                result.append(modifySql);
            }
        }
        return result.toString();
    }

    private String convertColumnsToDrop(String tableName, List<Field> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        
        // SQL Server删除列时，如果列有DEFAULT约束，需要先删除约束
        // 格式: 先删除约束，然后删除列
        StringBuilder result = new StringBuilder();
        org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate sqlServerTemplateImpl = 
            (org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate) sqlServerTemplate;
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append("; ");
            }
            Field column = columns.get(i);
            String columnName = column.getName();
            
            // 1. 先删除该列的 DEFAULT 约束（如果存在）
            result.append(sqlServerTemplateImpl.buildDropDefaultConstraintSql(tableName, columnName));
            
            // 2. 然后删除列
            result.append("; ");
            result.append(sqlServerTemplate.buildDropColumnSql(tableName, columnName));
        }
        return result.toString();
    }
}
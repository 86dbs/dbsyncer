/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * MySQL特定SQL模板实现
 */
public class MySQLTemplate implements SqlTemplate {

    @Override
    public String getLeftQuotation() {
        return "`";
    }

    @Override
    public String getRightQuotation() {
        return "`";
    }

    @Override
    public String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
        String fieldNames = fields.stream()
                .map(field -> buildColumn(field.getName()))
                .collect(java.util.stream.Collectors.joining(", "));
        String placeholders = fields.stream()
                .map(field -> "?")
                .collect(java.util.stream.Collectors.joining(", "));
        String updateClause = fields.stream()
                .filter(field -> !field.isPk())
                .map(field -> buildColumn(field.getName()) + " = VALUES(" + buildColumn(field.getName()) + ")")
                .collect(java.util.stream.Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                schemaTable, fieldNames, placeholders, updateClause);
    }

    /**
     * 构建MySQL添加列的SQL语句
     *
     * @param tableName 表名
     * @param field     字段信息
     * @return 添加列的SQL语句
     */
    public String buildAddColumnSql(String tableName, Field field) {
        return String.format("ALTER TABLE %s ADD COLUMN %s %s",
                buildQuotedTableName(tableName),
                buildColumn(field.getName()),
                convertToMySQLType(field));
    }

    /**
     * 构建MySQL修改列的SQL语句
     *
     * @param tableName 表名
     * @param field     字段信息
     * @return 修改列的SQL语句
     */
    public String buildModifyColumnSql(String tableName, Field field) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s",
                buildQuotedTableName(tableName),
                buildColumn(field.getName()),
                convertToMySQLType(field));
    }

    /**
     * 构建MySQL重命名列的SQL语句
     *
     * @param tableName    表名
     * @param oldFieldName 原字段名
     * @param newField     字段信息
     * @return 重命名列的SQL语句
     */
    public String buildChangeColumnSql(String tableName, String oldFieldName, Field newField) {
        return String.format("ALTER TABLE %s CHANGE COLUMN %s %s %s",
                buildQuotedTableName(tableName),
                buildColumn(oldFieldName),
                buildColumn(newField.getName()),
                convertToMySQLType(newField));
    }

    /**
     * 构建MySQL删除列的SQL语句
     *
     * @param tableName 表名
     * @param fieldName 字段名
     * @return 删除列的SQL语句
     */
    public String buildDropColumnSql(String tableName, String fieldName) {
        return String.format("ALTER TABLE %s DROP COLUMN %s",
                buildQuotedTableName(tableName),
                buildColumn(fieldName));
    }

    /**
     * 将中间表示的字段类型转换为MySQL特定的类型
     *
     * @param column 字段信息
     * @return MySQL特定的类型字符串
     */
    public String convertToMySQLType(Field column) {
        // 从类型名解析标准类型枚举
        DataTypeEnum standardType = DataTypeEnum.valueOf(column.getTypeName());
        if (standardType == null) {
            return "VARCHAR(255)"; // 默认类型
        }

        switch (standardType) {
            case STRING:
                if (column.getColumnSize() > 0) {
                    return "VARCHAR(" + column.getColumnSize() + ")";
                }
                return "VARCHAR(255)";
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case DECIMAL:
                if (column.getColumnSize() > 0 && column.getRatio() >= 0) {
                    return "DECIMAL(" + column.getColumnSize() + "," + column.getRatio() + ")";
                } else if (column.getColumnSize() > 0) {
                    return "DECIMAL(" + column.getColumnSize() + ")";
                }
                return "DECIMAL(10,0)";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "DATETIME";
            case BOOLEAN:
                return "TINYINT(1)";
            case BYTES:
                return "LONGBLOB";
            default:
                return "VARCHAR(255)"; // 默认类型
        }
    }
}
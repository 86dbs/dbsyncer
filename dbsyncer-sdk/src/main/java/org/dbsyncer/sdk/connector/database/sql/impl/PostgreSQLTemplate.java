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
            // PostgreSQL 11+ 语法要求：向非空表添加 NOT NULL 列时，必须提供 DEFAULT 值
            // 注意：这是为了满足 PostgreSQL 的语法约束，不是通用的缺省值处理
            // 生成的 DEFAULT 值仅用于满足语法要求，不会影响数据同步结果
            String defaultValue = PostgreSQLTemplate.getDefaultValueForNotNullColumn(field);
            if (defaultValue != null) {
                sql.append(" DEFAULT ").append(defaultValue);
            }
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
    public String buildCreateTableSql(String schema, String tableName, List<Field> fields, List<String> primaryKeys) {
        List<String> columnDefs = new ArrayList<>();
        for (Field field : fields) {
            String ddlType = convertToDatabaseType(field);
            String columnName = buildColumn(field.getName());
            
            // PostgreSQL 的自增字段处理：对于自增主键，使用 SERIAL 或 BIGSERIAL
            String typeDef;
            if (field.isAutoincrement() && field.isPk()) {
                if (ddlType.equalsIgnoreCase("INT") || ddlType.equalsIgnoreCase("INTEGER")) {
                    typeDef = "SERIAL";
                } else if (ddlType.equalsIgnoreCase("BIGINT")) {
                    typeDef = "BIGSERIAL";
                } else {
                    typeDef = ddlType;
                }
            } else {
                typeDef = ddlType;
            }
            
            // 构建列定义：列名 类型 [NOT NULL]
            // 注意：不再支持 DEFAULT 值，因为数据同步不需要默认值支持
            StringBuilder columnDef = new StringBuilder();
            columnDef.append(String.format("  %s %s", columnName, typeDef));
            
            // 添加 NOT NULL 约束（自增字段通常已经隐含 NOT NULL）
            if (!field.isAutoincrement() && field.getNullable() != null && !field.getNullable()) {
                columnDef.append(" NOT NULL");
            }
            
            columnDefs.add(columnDef.toString());
        }
        
        // 构建主键定义（如果主键不是自增的，或者自增字段已经隐含了主键约束）
        String pkClause = "";
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            // 检查是否所有主键都是自增的
            boolean allPkAutoIncrement = primaryKeys.stream()
                    .allMatch(pk -> fields.stream()
                            .anyMatch(f -> f.getName().equals(pk) && f.isAutoincrement()));
            
            // 如果主键不是自增的，需要显式定义 PRIMARY KEY
            if (!allPkAutoIncrement) {
                String pkColumns = primaryKeys.stream()
                        .map(this::buildColumn)
                        .collect(java.util.stream.Collectors.joining(", "));
                pkClause = String.format(",\n  PRIMARY KEY (%s)", pkColumns);
            }
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

    /**
     * 根据字段类型获取 NOT NULL 列的默认值
     * 
     * 注意：此方法仅用于满足 PostgreSQL 的语法约束，不是通用的缺省值处理。
     * PostgreSQL 11+ 要求：向非空表添加 NOT NULL 列时，必须提供 DEFAULT 值。
     * 
     * 背景说明：
     * - 项目在 2.7.0 版本取消了通用的缺省值处理（见 release-log.md），因为各数据库缺省值函数表达差异很大
     * - 但 PostgreSQL 11+ 的语法要求必须提供 DEFAULT 值，否则 DDL 执行会失败
     * - 此方法生成的 DEFAULT 值仅用于满足语法要求，不会影响数据同步结果（数据同步不依赖缺省值）
     * 
     * @param field 字段信息
     * @return 默认值表达式，如果不支持则返回 null
     */
    public static String getDefaultValueForNotNullColumn(Field field) {
        if (field == null || field.getTypeName() == null) {
            return null;
        }
        
        String typeName = field.getTypeName().toUpperCase();
        
        // 字符串类型：使用空字符串
        if (typeName.contains("VARCHAR") || typeName.contains("CHAR") || 
            typeName.contains("TEXT")) {
            return "''";
        }
        
        // 数值类型：使用 0
        if (typeName.contains("INT") || typeName.contains("INTEGER") ||
            typeName.contains("BIGINT") || typeName.contains("SMALLINT") ||
            typeName.contains("DECIMAL") || typeName.contains("NUMERIC") ||
            typeName.contains("REAL") || typeName.contains("DOUBLE") ||
            typeName.contains("FLOAT")) {
            return "0";
        }
        
        // 布尔类型：使用 false
        if (typeName.equals("BOOLEAN") || typeName.equals("BOOL")) {
            return "false";
        }
        
        // 日期时间类型：使用 '1900-01-01' 或 '1900-01-01 00:00:00'
        if (typeName.contains("DATE") || typeName.contains("TIME")) {
            if (typeName.contains("TIMESTAMP")) {
                return "'1900-01-01 00:00:00'";
            }
            return "'1900-01-01'";
        }
        
        // 二进制类型：使用 ''::bytea（空二进制）
        if (typeName.contains("BYTEA") || typeName.contains("BINARY")) {
            return "''::bytea";
        }
        
        // 其他类型：返回 null，让调用者决定如何处理
        return null;
    }

    @Override
    public String buildMetadataCountSql(String schema, String tableName) {
        // 转义单引号防止SQL注入
        String escapedTableName = tableName.replace("'", "''");
        String schemaName = (schema != null && !schema.trim().isEmpty()) ? schema.replace("'", "''") : "public";
        
        return String.format(
            "SELECT reltuples::BIGINT AS row_count FROM pg_class " +
            "WHERE relname = '%s' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '%s')",
            escapedTableName,
            schemaName
        );
    }

}
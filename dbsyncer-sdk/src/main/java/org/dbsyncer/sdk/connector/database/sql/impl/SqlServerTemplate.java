package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL Server特定SQL模板实现
 */
public class SqlServerTemplate extends AbstractSqlTemplate {

    public SqlServerTemplate(SchemaResolver schemaResolver) {
        super(schemaResolver);
    }

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
            return String.format("SELECT %s FROM %s WITH (NOLOCK) %s%s", fieldList, schemaTable, queryFilter, orderByClause);
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
        
        // 如果所有字段都是主键，updateClause 为空，需要至少添加一个更新表达式以满足 SQL Server 语法要求
        // 使用第一个主键字段的虚拟更新（不会实际改变值）
        if (updateClause.isEmpty() && !fields.isEmpty()) {
            Field firstField = fields.get(0);
            updateClause = "target." + buildColumn(firstField.getName()) + " = source." + buildColumn(firstField.getName());
        }

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

    @Override
    public String buildAddColumnSql(String tableName, Field field) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" ADD ").append(buildColumn(field.getName()))
           .append(" ").append(convertToDatabaseType(field));
        
        // 添加 NOT NULL 约束
        if (field.getNullable() != null && !field.getNullable()) {
            sql.append(" NOT NULL");
        }
        
        // 注意：不再支持 DEFAULT 值，因为数据同步不需要默认值支持
        
        // SQL Server 的注释需要使用扩展属性，这里暂时不处理
        // 如果需要添加注释，可以使用：
        // EXEC sp_addextendedproperty 'MS_Description', 'comment', 'SCHEMA', 'dbo', 'TABLE', tableName, 'COLUMN', fieldName;
        
        return sql.toString();
    }

    @Override
    public String buildModifyColumnSql(String tableName, Field field) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" ALTER COLUMN ").append(buildColumn(field.getName()))
           .append(" ").append(convertToDatabaseType(field));
        
        // 添加 NOT NULL 约束
        if (field.getNullable() != null && !field.getNullable()) {
            sql.append(" NOT NULL");
        }
        
        // SQL Server 的 ALTER COLUMN 不支持在同一个语句中修改 DEFAULT 值
        // 如果需要修改 DEFAULT，需要使用单独的 ALTER TABLE ... ADD CONSTRAINT 语句
        // 这里暂时不处理 DEFAULT 值的修改
        
        return sql.toString();
    }

    @Override
    public String buildRenameColumnSql(String tableName, String oldFieldName, Field newField) {
        return String.format("EXEC sp_rename '%s.%s', '%s', 'COLUMN'",
                tableName, oldFieldName, newField.getName());
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
            
            // 构建列定义：列名 类型 [NOT NULL] [IDENTITY(1,1)]
            // 注意：不再支持 DEFAULT 值，因为数据同步不需要默认值支持
            StringBuilder columnDef = new StringBuilder();
            columnDef.append(String.format("  %s %s", columnName, ddlType));
            
            if (field.getNullable() != null && !field.getNullable()) {
                columnDef.append(" NOT NULL");
            }
            
            if (field.isAutoincrement()) {
                columnDef.append(" IDENTITY(1,1)");
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
    public String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
        return "";
    }


    @Override
    public String convertToDatabaseType(Field column) {
        // 使用SchemaResolver进行类型转换，完全委托给SchemaResolver处理
        Field targetField = schemaResolver.fromStandardType(column);
        String typeName = targetField.getTypeName();
        
        // 获取标准类型名称，用于判断是否需要使用 MAX
        String standardType = column.getTypeName();
        
        // 在 IR to Target 阶段使用 isSizeFixed 信息，区分固定长度和可变长度字符串
        // 例如：MySQL的CHAR（固定长度）应该转换为SQL Server的NCHAR，而不是NVARCHAR
        Boolean isSizeFixed = column.getIsSizeFixed();
        if (isSizeFixed != null && isSizeFixed) {
            // 如果字段是固定长度，且目标类型是NVARCHAR，则改为NCHAR
            if ("NVARCHAR".equals(typeName.toUpperCase())) {
                typeName = "NCHAR";
            } else if ("VARCHAR".equals(typeName.toUpperCase())) {
                typeName = "CHAR";
            }
        }

        // 处理参数（长度、精度等）
        switch (typeName.toUpperCase()) {
            case "NCHAR":
            case "CHAR":
            case "NVARCHAR":
            case "VARCHAR":
                // 检查标准类型，如果是 JSON、TEXT 或 UNICODE_TEXT，使用 MAX
                if ("JSON".equals(standardType) || "TEXT".equals(standardType) || "UNICODE_TEXT".equals(standardType)) {
                    return typeName + "(MAX)";
                }
                // SQL Server 的 VARCHAR/NVARCHAR 最大长度是 8000，超过 8000 必须使用 MAX
                // CHAR/NCHAR 最大长度是 8000，超过 8000 必须使用 NVARCHAR(MAX)/VARCHAR(MAX)
                if (column.getColumnSize() > 0) {
                    if (column.getColumnSize() > 8000) {
                        // 如果长度超过 8000，对于固定长度类型，降级为可变长度类型
                        if ("NCHAR".equals(typeName.toUpperCase())) {
                            return "NVARCHAR(MAX)";
                        } else if ("CHAR".equals(typeName.toUpperCase())) {
                            return "VARCHAR(MAX)";
                        }
                        return typeName + "(MAX)";
                    }
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                throw new IllegalArgumentException("should give size for column: " + column.getTypeName());
            case "VARBINARY":
                // 根据标准类型判断：如果是BLOB类型，使用VARBINARY(MAX)；如果是BYTES类型，使用指定长度
                // column是标准类型字段，targetField是转换后的目标数据库类型字段
                if ("BLOB".equals(standardType)) {
                    // BLOB类型：使用VARBINARY(MAX)存储大容量二进制数据
                    return "VARBINARY(MAX)";
                } else {
                    // BYTES类型：小容量二进制数据，使用指定长度
                    if (column.getColumnSize() > 0 && column.getColumnSize() <= 8000) {
                        return typeName + "(" + column.getColumnSize() + ")";
                    } else if (column.getColumnSize() > 8000) {
                        // 如果长度超过8000，使用MAX（实际上应该使用BLOB类型）
                        return "VARBINARY(MAX)";
                    }
                    // 没有长度信息，默认使用VARBINARY(8000)作为小容量二进制
                    return "VARBINARY(8000)";
                }
            case "BINARY":
                // BINARY类型：固定长度二进制数据
                if (column.getColumnSize() > 0 && column.getColumnSize() <= 8000) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                // 没有长度信息或超过8000，默认使用BINARY(8000)
                return "BINARY(8000)";
            case "DECIMAL":
                if (column.getColumnSize() > 0 && column.getRatio() >= 0) {
                    return typeName + "(" + column.getColumnSize() + "," + column.getRatio() + ")";
                } else if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                return "DECIMAL(18,0)";
            default:
                return typeName;
        }
    }

    /**
     * 构建 SQL Server 列注释 SQL
     * 使用扩展属性存储注释，如果注释已存在则更新，不存在则添加
     * 
     * @param schema 架构名（如 'dbo'）
     * @param tableName 表名
     * @param columnName 列名
     * @param comment 注释内容
     * @return COMMENT SQL 语句
     */
    public String buildCommentSql(String schema, String tableName, String columnName, String comment) {
        // 转义单引号
        String escapedComment = comment.replace("'", "''");
        // 转义表名和列名中的单引号（虽然通常不会有，但为了安全）
        String escapedTableName = tableName.replace("'", "''");
        String escapedColumnName = columnName.replace("'", "''");
        
        // 使用 IF EXISTS 检查并更新/添加 COMMENT
        // 注意：EXEC 语句中不使用分号，因为整个 IF-ELSE 块应该作为一个完整的语句执行
        return String.format(
            "IF EXISTS (SELECT 1 FROM sys.extended_properties " +
            "WHERE major_id = OBJECT_ID('%s.%s') " +
            "  AND minor_id = COLUMNPROPERTY(OBJECT_ID('%s.%s'), '%s', 'ColumnId') " +
            "  AND name = 'MS_Description') " +
            "BEGIN " +
            "  EXEC sp_updateextendedproperty 'MS_Description', '%s', 'SCHEMA', '%s', 'TABLE', '%s', 'COLUMN', '%s' " +
            "END " +
            "ELSE " +
            "BEGIN " +
            "  EXEC sp_addextendedproperty 'MS_Description', '%s', 'SCHEMA', '%s', 'TABLE', '%s', 'COLUMN', '%s' " +
            "END",
            schema, escapedTableName, schema, escapedTableName, escapedColumnName,
            escapedComment, schema, escapedTableName, escapedColumnName,
            escapedComment, schema, escapedTableName, escapedColumnName
        );
    }

    /**
     * 构建删除 DEFAULT 约束的 SQL
     * 需要先查询约束名，然后删除
     * 
     * @param tableName 表名
     * @param columnName 列名
     * @return 删除约束的 SQL 语句
     */
    public String buildDropDefaultConstraintSql(String tableName, String columnName) {
        String quotedTableName = buildQuotedTableName(tableName);
        
        // 注意：整个语句作为一个批处理执行，DECLARE 和后续语句之间不使用分号
        // 这样按分号分割时，整个语句会作为一个完整的批处理执行
        return String.format(
            "DECLARE @constraintName NVARCHAR(200) " +
            "SELECT @constraintName = name FROM sys.default_constraints " +
            "WHERE parent_object_id = OBJECT_ID('%s') " +
            "  AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('%s'), '%s', 'ColumnId') " +
            "IF @constraintName IS NOT NULL " +
            "  EXEC('ALTER TABLE %s DROP CONSTRAINT ' + @constraintName)",
            tableName, tableName, columnName, quotedTableName
        );
    }

    /**
     * 构建添加 DEFAULT 约束的 SQL
     * 
     * @param tableName 表名
     * @param columnName 列名
     * @param defaultValue 默认值
     * @return 添加约束的 SQL 语句
     */
    public String buildAddDefaultConstraintSql(String tableName, String columnName, String defaultValue) {
        String quotedTableName = buildQuotedTableName(tableName);
        String quotedColumnName = buildColumn(columnName);
        // 约束名格式：DF_表名_列名
        String constraintName = "DF_" + tableName + "_" + columnName;
        
        return String.format(
            "ALTER TABLE %s ADD CONSTRAINT %s DEFAULT %s FOR %s;",
            quotedTableName, constraintName, defaultValue, quotedColumnName
        );
    }

    @Override
    public String buildMetadataCountSql(String schema, String tableName) {
        // 转义单引号防止SQL注入
        String escapedTableName = tableName.replace("'", "''");
        String schemaName = (schema != null && !schema.trim().isEmpty()) ? schema.replace("'", "''") : "dbo";
        
        return String.format(
            "SELECT p.rows FROM sys.tables t " +
            "INNER JOIN sys.partitions p ON t.object_id = p.object_id " +
            "WHERE t.name = '%s' AND t.schema_id = SCHEMA_ID('%s') AND p.index_id IN (0, 1)",
            escapedTableName,
            schemaName
        );
    }
}
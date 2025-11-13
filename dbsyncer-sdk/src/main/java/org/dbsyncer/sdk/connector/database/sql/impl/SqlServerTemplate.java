package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;

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
        return String.format("ALTER TABLE %s ADD %s %s",
                buildQuotedTableName(tableName),
                buildColumn(field.getName()),
                convertToDatabaseType(field));
    }

    @Override
    public String buildModifyColumnSql(String tableName, Field field) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s %s",
                buildQuotedTableName(tableName),
                buildColumn(field.getName()),
                convertToDatabaseType(field));
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
}
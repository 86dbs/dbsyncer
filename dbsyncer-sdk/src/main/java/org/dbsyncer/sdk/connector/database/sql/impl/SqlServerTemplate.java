package org.dbsyncer.sdk.connector.database.sql.impl;

import org.checkerframework.checker.nullness.qual.NonNull;
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

        // 构建 UPDATE SET 子句（排除主键字段和标识列）
        // 注意：即使开启了 IDENTITY_INSERT，MERGE 语句的 UPDATE 部分仍然不能更新标识列
        // 这是 SQL Server 的限制，因此需要在 UPDATE 子句中排除所有标识列
        String updateClause = fields.stream()
                .filter(field -> !primaryKeys.contains(field.getName()) && !field.isAutoincrement())
                .map(field -> "target." + buildColumn(field.getName()) + " = source." + buildColumn(field.getName()))
                .collect(java.util.stream.Collectors.joining(", "));
        
        // 如果所有字段都是主键或标识列，updateClause 为空，需要至少添加一个更新表达式以满足 SQL Server 语法要求
        // 使用第一个非标识列字段的虚拟更新（不会实际改变值）
        if (updateClause.isEmpty() && !fields.isEmpty()) {
            Field firstNonIdentityField = fields.stream()
                    .filter(field -> !field.isAutoincrement())
                    .findFirst()
                    .orElse(fields.get(0));
            updateClause = "target." + buildColumn(firstNonIdentityField.getName()) + " = source." + buildColumn(firstNonIdentityField.getName());
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
           .append(" ADD ").append(buildColumn(field.getName()));
        
        // 转换类型并获取 SQL Server 类型字符串
        String databaseType = convertToDatabaseType(field);
        sql.append(" ").append(databaseType);
        
        // 添加 NOT NULL 约束
        if (field.getNullable() != null && !field.getNullable()) {
            sql.append(" NOT NULL");
            // SQL Server 语法要求：向非空表添加 NOT NULL 列时，必须提供 DEFAULT 值
            // 注意：这是为了满足 SQL Server 的语法约束，不是通用的缺省值处理
            // 生成的 DEFAULT 值仅用于满足语法要求，不会影响数据同步结果
            // 使用转换后的 SQL Server 类型名称来判断默认值
            String defaultValue = getDefaultValueForNotNullColumnByTypeName(databaseType);
            if (defaultValue != null) {
                sql.append(" DEFAULT ").append(defaultValue);
            }
        }
        
        // SQL Server 的注释需要使用扩展属性，这里暂时不处理
        // 如果需要添加注释，可以使用：
        // EXEC sp_addextendedproperty 'MS_Description', 'comment', 'SCHEMA', 'dbo', 'TABLE', tableName, 'COLUMN', fieldName;
        
        return sql.toString();
    }
    
    /**
     * 根据 SQL Server 数据库类型名称获取 NOT NULL 列的默认值
     * 
     * 注意：此方法仅用于满足 SQL Server 的语法约束，不是通用的缺省值处理。
     * SQL Server 要求：向非空表添加 NOT NULL 列时，必须提供 DEFAULT 值。
     * 
     * 背景说明：
     * - 项目在 2.7.0 版本取消了通用的缺省值处理（见 release-log.md），因为各数据库缺省值函数表达差异很大
     * - 但 SQL Server 的语法要求必须提供 DEFAULT 值，否则 DDL 执行会失败
     * - 此方法生成的 DEFAULT 值仅用于满足语法要求，不会影响数据同步结果（数据同步不依赖缺省值）
     * 
     * @param typeName SQL Server 数据库类型名称（如 "BIGINT", "NVARCHAR(50)" 等）
     * @return 默认值表达式，如果不支持则返回 null
     */
    public static String getDefaultValueForNotNullColumnByTypeName(String typeName) {
        if (typeName == null || typeName.trim().isEmpty()) {
            return null;
        }
        
        String upperTypeName = typeName.toUpperCase();
        
        // 字符串类型：使用空字符串
        if (upperTypeName.contains("VARCHAR") || upperTypeName.contains("CHAR") || 
            upperTypeName.contains("TEXT") || upperTypeName.contains("NCHAR") || 
            upperTypeName.contains("NVARCHAR")) {
            // Unicode 字符串类型使用 N''
            if (upperTypeName.contains("NCHAR") || upperTypeName.contains("NVARCHAR") || 
                upperTypeName.contains("NTEXT")) {
                return "N''";
            }
            return "''";
        }
        
        // 数值类型：使用 0
        if (upperTypeName.contains("INT") || upperTypeName.contains("BIGINT") || 
            upperTypeName.contains("SMALLINT") || upperTypeName.contains("TINYINT") ||
            upperTypeName.contains("DECIMAL") || upperTypeName.contains("NUMERIC") ||
            upperTypeName.contains("FLOAT") || upperTypeName.contains("REAL") ||
            upperTypeName.contains("MONEY") || upperTypeName.contains("SMALLMONEY")) {
            return "0";
        }
        
        // 布尔类型（BIT）：使用 0
        if (upperTypeName.equals("BIT")) {
            return "0";
        }
        
        // 日期时间类型：使用 '1900-01-01'
        // 注意：对于 DATETIME2 和 DATETIMEOFFSET，SQL Server 会接受 '1900-01-01' 并自动补全时间部分为 '00:00:00'
        // 但为了与测试期望一致（测试验证时会标准化比较，只检查日期部分），统一使用 '1900-01-01'
        if (upperTypeName.contains("DATE") || upperTypeName.contains("TIME")) {
            return "'1900-01-01'";
        }
        
        // 二进制类型：使用 0x（空二进制）
        if (upperTypeName.contains("BINARY") || upperTypeName.contains("VARBINARY") ||
            upperTypeName.contains("IMAGE")) {
            return "0x";
        }
        
        // 其他类型：返回 null，让调用者决定如何处理
        return null;
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
        
        // 去除类型名称中的 IDENTITY 关键字（如果存在）
        // IDENTITY 属性应该通过 field.isAutoincrement() 来判断，而不是从类型名称中获取
        // 这样可以避免在 buildCreateTableSql 中重复添加 IDENTITY(1,1)
        if (typeName != null) {
            typeName = typeName.replaceAll("(?i)\\s+IDENTITY", "").trim();
        }
        
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
            "IF EXISTS (SELECT 1 FROM sys.extended_properties WITH (NOLOCK) " +
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
            "SELECT @constraintName = name FROM sys.default_constraints WITH (NOLOCK) " +
            "WHERE parent_object_id = OBJECT_ID('%s') " +
            "  AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('%s'), '%s', 'ColumnId') " +
            "IF @constraintName IS NOT NULL " +
            "  EXEC('ALTER TABLE %s DROP CONSTRAINT ' + @constraintName)",
            tableName, tableName, columnName, quotedTableName
        );
    }

    @Override
    public String buildMetadataCountSql(String schema, String tableName) {
        // 转义单引号防止SQL注入
        String escapedTableName = tableName.replace("'", "''");
        String schemaName = (schema != null && !schema.trim().isEmpty()) ? schema.replace("'", "''") : "dbo";
        
        return String.format(
            "SELECT p.rows FROM sys.tables t WITH (NOLOCK) " +
            "INNER JOIN sys.partitions p  WITH (NOLOCK) ON t.object_id = p.object_id " +
            "WHERE t.name = '%s' AND t.schema_id = SCHEMA_ID('%s') AND p.index_id IN (0, 1)",
            escapedTableName,
            schemaName
        );
    }

    /**
     * 构建获取数据库名称的 SQL
     * 
     * @return SQL 语句
     */
    public String buildGetDatabaseNameSql() {
        return "SELECT db_name()";
    }

    /**
     * 构建获取表列表的 SQL
     * 
     * @param schema 架构名（如 'dbo'），如果为 null 或空则使用当前架构
     * @return SQL 语句（包含占位符 '#'，需要调用者替换为实际的 schema）
     */
    public String buildGetTableListSql(String schema) {
        String schemaName = (schema != null && !schema.trim().isEmpty()) ? schema : "dbo";
        return String.format("SELECT name FROM sys.tables WITH (NOLOCK) WHERE schema_id = schema_id('%s') AND is_ms_shipped = 0", schemaName);
    }

    /**
     * 构建获取表主键和列数的合并查询 SQL
     * 使用合并查询减少 IO 开销，同时获取主键信息和列数
     * 
     * @param schema 架构名
     * @param tableName 表名
     * @return SQL 语句（使用 PreparedStatement 参数，需要设置 4 个参数：schema, tableName, schema, tableName）
     */
    public String buildGetPrimaryKeysAndColumnCountSql(String schema, String tableName) {
        // 转义单引号防止SQL注入
        String escapedSchema = (schema != null && !schema.trim().isEmpty()) ? schema.replace("'", "''") : "dbo";
        String escapedTableName = tableName.replace("'", "''");
        
        // 优化：使用 CROSS APPLY 替代相关子查询，避免对每一行主键记录都执行一次子查询
        // CROSS APPLY 会让 SQL Server 优化器识别出列数查询结果对所有行都相同，从而只执行一次
        // 注意：CROSS APPLY 从 SQL Server 2005 开始支持，SQL Server 2008 R2 完全支持
        return "SELECT " +
                "    kcu.COLUMN_NAME, " +
                "    cc.COLUMN_COUNT " +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc WITH (NOLOCK) " +
                "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu WITH (NOLOCK) " +
                "    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME " +
                "    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA " +
                "    AND tc.TABLE_NAME = kcu.TABLE_NAME " +
                "CROSS APPLY (" +
                "    SELECT COUNT(*) AS COLUMN_COUNT " +
                "    FROM INFORMATION_SCHEMA.COLUMNS c WITH (NOLOCK) " +
                "    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?" +
                ") AS cc " +
                String.format("WHERE tc.TABLE_SCHEMA = '%s' AND tc.TABLE_NAME = '%s' ", escapedSchema, escapedTableName) +
                "    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                "ORDER BY kcu.ORDINAL_POSITION";
    }

    // ==================== Change Tracking 相关 SQL 构建方法 ====================

    /**
     * Change Tracking 特殊列名，用于标识表结构信息的 JSON 字段（不用于数据同步）
     */
    public static final String CT_DDL_SCHEMA_INFO_COLUMN = "__DDL_SCHEMA_INFO__";

    /**
     * CT 主键列别名前缀（使用双下划线前缀避免与用户列名冲突）
     */
    public static final String CT_PK_COLUMN_PREFIX = "__CT_PK_";

    /**
     * 构建获取 Change Tracking 当前版本的 SQL
     * 
     * @return SQL 语句
     */
    public String buildGetCurrentVersionSql() {
        return "SELECT CHANGE_TRACKING_CURRENT_VERSION()";
    }

    /**
     * 构建获取表结构信息的子查询 SQL
     * 用于在 DML 查询中附加表结构信息
     * 注意：SQL Server 2008 R2 不支持 FOR JSON PATH，使用字符串拼接方式生成 JSON
     * 使用 STUFF + FOR XML PATH 来拼接 JSON 数组字符串
     * 
     * @param schema 架构名
     * @param tableName 表名
     * @return 子查询 SQL
     */
    public String buildGetTableSchemaInfoSubquery(String schema, String tableName) {
        // 转义单引号防止SQL注入
        String escapedSchema = (schema != null && !schema.trim().isEmpty()) ? schema.replace("'", "''") : "dbo";
        String escapedTableName = tableName.replace("'", "''");
        
        // 优化：移除外层包装，因为现在在 CROSS APPLY 中使用
        // CROSS APPLY 需要一个返回单行单列的表值表达式
        return String.format(
                "(" + schemaSql() + ")",
                escapedSchema, escapedTableName
        );
    }

    public String buildSchemeOnly(String schema, String tableName) {
        // 转义单引号防止SQL注入
        String escapedSchema = (schema != null && !schema.trim().isEmpty()) ? schema.replace("'", "''") : "dbo";
        String escapedTableName = tableName.replace("'", "''");

        // 优化：移除外层包装，因为现在在 CROSS APPLY 中使用
        // CROSS APPLY 需要一个返回单行单列的表值表达式
        return String.format(schemaSql(), escapedSchema, escapedTableName);
    }

    private static @NonNull String schemaSql() {
        return "SELECT " +
                "    '[' + STUFF((" +
                "        SELECT ',' + " +
                "            '{' + " +
                "            '\"COLUMN_NAME\":\"' + REPLACE(COLUMN_NAME, '\"', '\\\"') + '\",' + " +
                "            '\"DATA_TYPE\":\"' + REPLACE(DATA_TYPE, '\"', '\\\"') + '\",' + " +
                "            CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL " +
                "                THEN '\"CHARACTER_MAXIMUM_LENGTH\":' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ',' " +
                "                ELSE '' END + " +
                "            CASE WHEN NUMERIC_PRECISION IS NOT NULL " +
                "                THEN '\"NUMERIC_PRECISION\":' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' " +
                "                ELSE '' END + " +
                "            CASE WHEN NUMERIC_SCALE IS NOT NULL " +
                "                THEN '\"NUMERIC_SCALE\":' + CAST(NUMERIC_SCALE AS VARCHAR) + ',' " +
                "                ELSE '' END + " +
                "            '\"IS_NULLABLE\":\"' + IS_NULLABLE + '\",' + " +
                "            '\"ORDINAL_POSITION\":' + CAST(ORDINAL_POSITION AS VARCHAR) + " +
                "            '}' " +
                "        FROM INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK) " +
                "        WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' " +
                "        ORDER BY ORDINAL_POSITION " +
                "        FOR XML PATH(''), TYPE " +
                "    ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') + ']' AS schema_info";
    }

    /**
     * 构建 Change Tracking DML 变更查询的主查询 SQL
     * 
     * @param schema 架构名
     * @param tableName 表名
     * @param primaryKeys 主键列表
     * @param schemaInfoSubquery 表结构信息子查询
     * @return SQL 语句（使用 PreparedStatement 参数，需要设置 3 个参数：startVersion, startVersion, stopVersion）
     */
    public String buildChangeTrackingDMLMainQuery(String schema, String tableName, 
                                                   List<String> primaryKeys, String schemaInfoSubquery) {
        // 构建 JOIN 条件（支持复合主键）
        // buildColumn 已经包含了引号，所以不需要再手动添加
        String joinCondition = primaryKeys.stream()
                .map(pk -> "CT." + buildColumn(pk) + " = T." + buildColumn(pk))
                .collect(java.util.stream.Collectors.joining(" AND "));

        // 构建 SELECT 列列表
        // 将 CT 主键列放在 T.* 前面，简化后续处理（可以通过固定索引直接访问）
        // 对于主键列：如果 T.[pk] 为 NULL（DELETE 情况），使用 CT.[pk]
        // 使用双下划线前缀避免与用户列名冲突
        String redundantPrimaryKeys = primaryKeys.stream()
                .map(pk -> "CT." + buildColumn(pk) + " AS " + CT_PK_COLUMN_PREFIX + pk.replaceAll("[^a-zA-Z0-9_]", "_"))
                .collect(java.util.stream.Collectors.joining(", "));

        // 将 CT 主键列放在 T.* 前面
        String selectColumns = (redundantPrimaryKeys.isEmpty() ? "" : redundantPrimaryKeys + ", ") + "T.*";

        // 构建表名（带引号）
        String schemaTable = buildTable(schema, tableName);

        // 优化：使用 CROSS APPLY 让表结构信息子查询只执行一次，而不是每行都执行
        // CROSS APPLY 会将子查询结果与主查询结果集进行关联，SQL Server 优化器会识别出
        // 子查询结果对所有行都相同，从而只执行一次
        // 注意：CROSS APPLY 从 SQL Server 2005 开始支持，SQL Server 2008 R2 完全支持
        // 注意：CROSS APPLY 的别名语法是 "CROSS APPLY (...) alias"，不使用 AS 关键字
        return String.format(
                "SELECT " +
                        "    CT.SYS_CHANGE_VERSION, " +
                        "    CT.SYS_CHANGE_OPERATION, " +
                        "    CT.SYS_CHANGE_COLUMNS, " +
                        "    %s, " +  // 显式的列列表
                        "    SI.schema_info AS " + CT_DDL_SCHEMA_INFO_COLUMN + " " +
                        "FROM CHANGETABLE(CHANGES %s, ?) AS CT " +
                        "LEFT JOIN %s AS T WITH (NOLOCK) ON %s " +
                        "CROSS APPLY %s SI " +  // 使用 CROSS APPLY，注意：某些 SQL Server 版本不支持 AS 关键字
                        "WHERE CT.SYS_CHANGE_VERSION > ? AND CT.SYS_CHANGE_VERSION <= ? " +
                        "ORDER BY CT.SYS_CHANGE_VERSION ASC",
                selectColumns,         // 显式的列列表
                schemaTable,          // CHANGETABLE
                schemaTable,          // JOIN table
                joinCondition,        // JOIN condition（支持复合主键）
                schemaInfoSubquery    // 表结构信息子查询（移到 CROSS APPLY）
        );
    }

    /**
     * 构建 Change Tracking DML 变更查询的辅助查询 SQL
     * 当没有 DML 变更时，用于获取 DDL 信息
     * 
     * @param schema 架构名
     * @param tableName 表名
     * @param schemaInfoSubquery 表结构信息子查询
     * @return SQL 语句（使用 PreparedStatement 参数，需要设置 3 个参数：startVersion, startVersion, stopVersion）
     */
    public String buildChangeTrackingDMLFallbackQuery(String schema, String tableName, String schemaInfoSubquery) {
        String schemaTable = buildTable(schema, tableName);
        
        // 优化：使用 CROSS APPLY 让子查询只执行一次
        // 注意：需要一个 FROM 子句才能使用 CROSS APPLY，使用虚拟表 (SELECT 1 AS dummy) AS t
        // 注意：CROSS APPLY 的别名语法是 "CROSS APPLY (...) alias"，不使用 AS 关键字
        return String.format(
                "SELECT SI.schema_info AS " + CT_DDL_SCHEMA_INFO_COLUMN + " " +
                        "FROM (SELECT 1 AS dummy) AS t " +
                        "CROSS APPLY %s SI " +  // 使用 CROSS APPLY，注意：某些 SQL Server 版本不支持 AS 关键字
                        "WHERE NOT EXISTS (" +
                        "    SELECT 1 FROM CHANGETABLE(CHANGES %s, ?) AS CT " +
                        "    WHERE CT.SYS_CHANGE_VERSION > ? AND CT.SYS_CHANGE_VERSION <= ?" +
                        ")",
                schemaInfoSubquery,   // 表结构信息子查询
                schemaTable           // CHANGETABLE（用于 EXISTS 子查询）
        );
    }

    /**
     * 构建启用数据库 Change Tracking 的 SQL
     * 
     * @param databaseName 数据库名
     * @return SQL 语句
     */
    public String buildEnableDatabaseChangeTrackingSql(String databaseName) {
        // 转义单引号防止SQL注入
        String escapedDatabaseName = databaseName.replace("'", "''");
        return String.format("ALTER DATABASE [%s] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)", 
                escapedDatabaseName);
    }

    /**
     * 构建启用表 Change Tracking 的 SQL
     * 
     * @param schema 架构名
     * @param tableName 表名
     * @return SQL 语句
     */
    public String buildEnableTableChangeTrackingSql(String schema, String tableName) {
        String schemaTable = buildTable(schema, tableName);
        return String.format("ALTER TABLE %s ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)", schemaTable);
    }

    /**
     * 构建检查数据库 Change Tracking 是否启用的 SQL
     * 
     * @param databaseName 数据库名
     * @return SQL 语句
     */
    public String buildIsDatabaseChangeTrackingEnabledSql(String databaseName) {
        // 转义单引号防止SQL注入
        String escapedDatabaseName = databaseName.replace("'", "''");
        return String.format("SELECT COUNT(*) FROM sys.change_tracking_databases WITH (NOLOCK) WHERE database_id = DB_ID('%s')", 
                escapedDatabaseName);
    }

    /**
     * 构建检查表 Change Tracking 是否启用的 SQL
     * 
     * @param schema 架构名
     * @param tableName 表名
     * @return SQL 语句
     */
    public String buildIsTableChangeTrackingEnabledSql(String schema, String tableName) {
        // 转义单引号防止SQL注入
        String escapedSchema = (schema != null && !schema.trim().isEmpty()) ? schema.replace("'", "''") : "dbo";
        String escapedTableName = tableName.replace("'", "''");
        return String.format("SELECT COUNT(*) FROM sys.change_tracking_tables WITH (NOLOCK) WHERE object_id = OBJECT_ID('%s.%s')", 
                escapedSchema, escapedTableName);
    }
}
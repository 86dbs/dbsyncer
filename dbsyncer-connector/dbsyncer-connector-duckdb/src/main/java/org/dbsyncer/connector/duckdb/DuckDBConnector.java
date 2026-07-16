/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.duckdb.schema.DuckDBSchemaResolver;
import org.dbsyncer.connector.duckdb.validator.DuckDBConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.BindParameter;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DuckDB 连接器（本地文件库，可作为 Mapping 源/目标及整库迁移目标）
 * <p>
 * 建库语义：连接配置 {@code serviceName} 指向 .duckdb 文件；整库迁移的 database/schema
 * 映射为文件内 {@code CREATE SCHEMA}（单文件多 schema），而非新建物理文件。
 * </p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 10:00
 */
public final class DuckDBConnector extends AbstractDatabaseConnector {

    private static final String QUERY_SCHEMA = "SELECT schema_name FROM information_schema.schemata "
                    + "WHERE lower(schema_name) NOT IN ('information_schema', 'pg_catalog') "
                    + "ORDER BY schema_name";

    private static final String QUERY_SCHEMA_EXISTS =
            "SELECT count(1) FROM information_schema.schemata WHERE schema_name = ?";

    private static final String QUERY_COLUMNS =
            "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
                    + "FROM information_schema.columns "
                    + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";

    private static final String QUERY_PRIMARY_KEYS =
            "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                    + "JOIN information_schema.key_column_usage kcu "
                    + "ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
                    + "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = ? AND tc.table_name = ? "
                    + "ORDER BY kcu.ordinal_position";

    private static final String QUERY_TABLES =
            "SELECT table_name, table_type FROM information_schema.tables "
                    + "WHERE table_schema = ? ORDER BY table_name";

    private final DuckDBConfigValidator configValidator = new DuckDBConfigValidator();
    private final DuckDBSchemaResolver schemaResolver = new DuckDBSchemaResolver();

    @Override
    public String getConnectorType() {
        return "DuckDB";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:duckdb:/path/to/file.duckdb ；空路径为内存库
        if (StringUtil.isBlank(database) || ":memory:".equalsIgnoreCase(database.trim())) {
            return "jdbc:duckdb:";
        }
        return "jdbc:duckdb:" + database.trim();
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        return null;
    }

    /**
     * UI「数据库」列表返回 schema；选择结果常写入 catalog，故解析时 catalog/schema 互通。
     */
    @Override
    protected String getCatalog(String database, Connection connection) {
        return null;
    }

    @Override
    protected String getSchema(String schema, Connection connection) throws java.sql.SQLException {
        if (StringUtil.isNotBlank(schema)) {
            return schema;
        }
        String current = connection.getSchema();
        return StringUtil.isNotBlank(current) ? current : "main";
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        String schema = resolveSchema(context);
        return connectorInstance.execute(databaseTemplate -> {
            List<Map<String, Object>> rows = databaseTemplate.queryForList(QUERY_TABLES, schema);
            if (CollectionUtils.isEmpty(rows)) {
                return new ArrayList<>();
            }
            List<Table> tables = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                String name = stringVal(row, "table_name", "TABLE_NAME");
                if (StringUtil.isBlank(name)) {
                    continue;
                }
                Table table = new Table();
                table.setName(name);
                table.setType(resolveTableType(stringVal(row, "table_type", "TABLE_TYPE")));
                tables.add(table);
            }
            return tables;
        });
    }

    @Override
    public List<MetaInfo> getMetaInfo(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        DefaultConnectorServiceContext fixed = new DefaultConnectorServiceContext();
        fixed.setCatalog(null);
        fixed.setSchema(resolveSchema(context));
        fixed.setTablePatterns(context.getTablePatterns());
        return super.getMetaInfo(connectorInstance, fixed);
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> {
            List<String> schemas = databaseTemplate.queryForList(QUERY_SCHEMA, String.class);
            if (CollectionUtils.isEmpty(schemas)) {
                return Collections.singletonList("main");
            }
            return schemas;
        });
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    /**
     * 整库迁移：在当前已连接的 DuckDB 文件内创建 schema（幂等）。
     * databaseName / schemaName 任一非空即可；优先 schemaName。
     */
    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        String name = StringUtil.isNotBlank(schemaName) ? schemaName : databaseName;
        if (StringUtil.isBlank(name)) {
            return StringUtil.EMPTY;
        }
        if ("main".equalsIgnoreCase(name.trim())) {
            return StringUtil.EMPTY;
        }
        return "CREATE SCHEMA IF NOT EXISTS " + buildWithQuotation(name.trim());
    }

    @Override
    public boolean databaseExists(DatabaseConnectorInstance connectorInstance, String databaseName, String schemaName) {
        String name = StringUtil.isNotBlank(schemaName) ? schemaName : databaseName;
        if (StringUtil.isBlank(name)) {
            return true;
        }
        if ("main".equalsIgnoreCase(name.trim())) {
            return true;
        }
        Integer count = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject(QUERY_SCHEMA_EXISTS, Integer.class, name.trim()));
        return count != null && count > 0;
    }

    /**
     * 跨库迁移建表：sourceDDL 为列定义片段（可含 PRIMARY KEY (...)）。
     */
    @Override
    public String getTargetTableDDL(DatabaseConnectorInstance targetInstance, String tableName, String sourceDDL) {
        if (StringUtil.isBlank(sourceDDL) || StringUtil.isBlank(tableName)) {
            return StringUtil.EMPTY;
        }
        return "CREATE TABLE IF NOT EXISTS " + qualifyTableName(targetInstance, tableName)
                + " (" + sourceDDL.trim() + ")";
    }

    /**
     * 同类型迁移：从 information_schema 拼装 CREATE TABLE。
     */
    @Override
    public String getSourceTableDDL(DatabaseConnectorInstance sourceInstance, String sourceTableName) {
        if (sourceInstance == null || StringUtil.isBlank(sourceTableName)) {
            return StringUtil.EMPTY;
        }
        String schema = StringUtil.getIfBlank(sourceInstance.getSchema(),
                StringUtil.getIfBlank(sourceInstance.getCatalog(), "main"));
        return sourceInstance.execute(databaseTemplate -> {
            List<Map<String, Object>> columns = databaseTemplate.queryForList(QUERY_COLUMNS, schema, sourceTableName);
            if (CollectionUtils.isEmpty(columns)) {
                return StringUtil.EMPTY;
            }
            List<String> pkColumns = databaseTemplate.queryForList(QUERY_PRIMARY_KEYS, String.class, schema, sourceTableName);
            List<String> columnDefs = new ArrayList<>();
            for (Map<String, Object> row : columns) {
                String colName = stringVal(row, "column_name", "COLUMN_NAME");
                String dataType = stringVal(row, "data_type", "DATA_TYPE");
                if (StringUtil.isBlank(colName) || StringUtil.isBlank(dataType)) {
                    continue;
                }
                StringBuilder def = new StringBuilder();
                def.append(buildWithQuotation(colName)).append(" ").append(formatDuckType(dataType, row));
                String nullable = stringVal(row, "is_nullable", "IS_NULLABLE");
                if ("NO".equalsIgnoreCase(nullable)) {
                    def.append(" NOT NULL");
                }
                columnDefs.add(def.toString());
            }
            if (CollectionUtils.isEmpty(columnDefs)) {
                return StringUtil.EMPTY;
            }
            if (!CollectionUtils.isEmpty(pkColumns)) {
                List<String> quotedPk = pkColumns.stream().map(this::buildWithQuotation).collect(Collectors.toList());
                columnDefs.add("PRIMARY KEY (" + StringUtil.join(quotedPk, StringUtil.COMMA) + ")");
            }
            return "CREATE TABLE " + qualifyTableName(sourceInstance, sourceTableName)
                    + " (" + StringUtil.join(columnDefs, StringUtil.COMMA) + ")";
        });
    }

    @Override
    public String buildDropTableSql(DatabaseConnectorInstance targetInstance, String tableName) {
        if (StringUtil.isBlank(tableName)) {
            return StringUtil.EMPTY;
        }
        return "DROP TABLE IF EXISTS " + qualifyTableName(targetInstance, tableName);
    }

    @Override
    public String getPageSql(PageSql config) {
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        appendOrderByPrimaryKeys(sql, config);
        sql.append(DatabaseConstant.SQLITE_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageIndex = context.getPageIndex();
        int pageSize = context.getPageSize();
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        buildCursorConditionAndOrderBy(sql, config);
        sql.append(DatabaseConstant.SQLITE_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors || cursors.length == 0) {
            return new Object[]{pageSize, 0};
        }
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{pageSize, 0};
        }
        Object[] newCursors = new Object[cursorArgs.length + 2];
        System.arraycopy(cursorArgs, 0, newCursors, 0, cursorArgs.length);
        newCursors[cursorArgs.length] = pageSize;
        newCursors[cursorArgs.length + 1] = 0;
        return newCursors;
    }

    @Override
    public String buildModifyColumnsSql(DatabaseConnectorInstance targetInstance, ValidateSyncTask task,
                                        String targetTableName, List<Field> sourceDefinitions,
                                        List<String> targetColumnNames) {
        throw new DuckDBException("DuckDB 暂不支持列类型订正 ALTER");
    }

    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        UpsertContext context = buildUpsertContext(config);
        return config.getDatabase().generateUniqueCode()
                + "INSERT OR IGNORE INTO " + config.getSchema()
                + config.getDatabase().buildWithQuotation(config.getTableName()) + "("
                + StringUtil.join(context.fieldNames, StringUtil.COMMA) + ") VALUES ("
                + StringUtil.join(context.valuePlaceholders, StringUtil.COMMA) + ")";
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        UpsertContext context = buildUpsertContext(config);
        if (CollectionUtils.isEmpty(context.pkFieldNames)) {
            return buildInsertSql(config);
        }
        StringBuilder sql = new StringBuilder(config.getDatabase().generateUniqueCode());
        sql.append("INSERT INTO ").append(config.getSchema());
        sql.append(config.getDatabase().buildWithQuotation(config.getTableName()));
        sql.append("(").append(StringUtil.join(context.fieldNames, StringUtil.COMMA)).append(") ");
        sql.append("VALUES (").append(StringUtil.join(context.valuePlaceholders, StringUtil.COMMA)).append(")");
        sql.append(" ON CONFLICT (").append(StringUtil.join(context.pkFieldNames, StringUtil.COMMA)).append(")");
        if (CollectionUtils.isEmpty(context.updateSets)) {
            sql.append(" DO NOTHING");
        } else {
            sql.append(" DO UPDATE SET ").append(StringUtil.join(context.updateSets, StringUtil.COMMA));
        }
        return sql.toString();
    }

    @Override
    public String formatPhysicalType(Field sourceDefinition) {
        String raw = sourceDefinition.getTypeName();
        if (StringUtil.isBlank(raw)) {
            return "VARCHAR";
        }
        String t = raw.trim().toUpperCase(Locale.ROOT);
        if (t.startsWith("HUGEINT") || t.startsWith("UHUGEINT")) {
            return "VARCHAR";
        }
        if (t.startsWith("LIST") || t.startsWith("STRUCT") || t.startsWith("MAP") || t.startsWith("UNION")) {
            return "VARCHAR";
        }
        if ("STRING".equals(t)) {
            return "VARCHAR";
        }
        if ("DATETIME".equals(t)) {
            return "TIMESTAMP";
        }
        if ("YEAR".equals(t)) {
            return "INTEGER";
        }
        if (t.endsWith("BLOB") || "BINARY".equals(t) || "VARBINARY".equals(t) || t.startsWith("BINARY") || t.startsWith("VARBINARY")) {
            return "BLOB";
        }
        return super.formatPhysicalType(sourceDefinition);
    }

    /**
     * DuckDB JDBC 的 setObject(sqlType) 不支持 Types.BLOB/VARBINARY（会报 Unknown target type）；
     * 二进制列改走 setBytes。
     */
    @Override
    protected Object wrapBindParameter(Field field, Object val) {
        if (!(val instanceof byte[])) {
            return val;
        }
        final byte[] bytes = (byte[]) val;
        return (BindParameter) (ps, paramIndex, connection) -> ps.setBytes(paramIndex, bytes);
    }

    private UpsertContext buildUpsertContext(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        UpsertContext context = new UpsertContext();
        config.getFields().forEach(f -> {
            String fieldName = database.buildWithQuotation(f.getName());
            context.fieldNames.add(fieldName);
            List<String> fieldVs = new ArrayList<>();
            if (database.buildCustomValue(fieldVs, f)) {
                context.valuePlaceholders.add(fieldVs.get(0));
            } else {
                context.valuePlaceholders.add("?");
            }
            if (f.isPk()) {
                context.pkFieldNames.add(fieldName);
            } else {
                context.updateSets.add(String.format("%s = excluded.%s", fieldName, fieldName));
            }
        });
        return context;
    }

    private String qualifyTableName(DatabaseConnectorInstance instance, String tableName) {
        String table = buildWithQuotation(tableName);
        if (instance == null) {
            return table;
        }
        // 实例 catalog 常承载 UI 所选 schema（getDatabases 返回 schema 列表）
        String schema = StringUtil.isNotBlank(instance.getSchema())
                ? instance.getSchema()
                : instance.getCatalog();
        if (StringUtil.isNotBlank(schema)) {
            return buildWithQuotation(schema) + "." + table;
        }
        return table;
    }

    private static String resolveSchema(ConnectorServiceContext context) {
        if (context == null) {
            return "main";
        }
        String schema = StringUtil.getIfBlank(context.getSchema(), context.getCatalog());
        return StringUtil.isNotBlank(schema) ? schema : "main";
    }

    private static String resolveTableType(String tableType) {
        if (StringUtil.isBlank(tableType)) {
            return TableTypeEnum.TABLE.getCode();
        }
        String type = tableType.trim().toUpperCase(Locale.ROOT);
        if (type.contains("VIEW") && type.contains("MATERIALIZED")) {
            return TableTypeEnum.MATERIALIZED_VIEW.getCode();
        }
        if (type.contains("VIEW")) {
            return TableTypeEnum.VIEW.getCode();
        }
        return TableTypeEnum.TABLE.getCode();
    }

    private static String formatDuckType(String dataType, Map<String, Object> row) {
        String type = dataType.trim().toUpperCase(Locale.ROOT);
        if (type.contains("CHAR") || "VARCHAR".equals(type) || "STRING".equals(type) || "TEXT".equals(type)) {
            int len = toNonNegativeInt(firstNonNull(row, "character_maximum_length", "CHARACTER_MAXIMUM_LENGTH"));
            if (len > 0 && !"TEXT".equals(type) && !"STRING".equals(type)) {
                return "VARCHAR(" + len + ")";
            }
            return "VARCHAR".equals(type) || "STRING".equals(type) || "TEXT".equals(type) ? "VARCHAR" : type;
        }
        if (type.contains("DECIMAL") || type.contains("NUMERIC")) {
            int precision = toNonNegativeInt(firstNonNull(row, "numeric_precision", "NUMERIC_PRECISION"));
            int scale = toNonNegativeInt(firstNonNull(row, "numeric_scale", "NUMERIC_SCALE"));
            if (precision > 0 && scale >= 0) {
                return String.format(Locale.ROOT, "DECIMAL(%d,%d)", precision, scale);
            }
            if (precision > 0) {
                return String.format(Locale.ROOT, "DECIMAL(%d)", precision);
            }
        }
        return type;
    }

    private static Object firstNonNull(Map<String, Object> row, String... keys) {
        if (row == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            Object val = row.get(key);
            if (val != null) {
                return val;
            }
        }
        return null;
    }

    private static String stringVal(Map<String, Object> row, String... keys) {
        Object val = firstNonNull(row, keys);
        return val == null ? null : String.valueOf(val);
    }

    private static int toNonNegativeInt(Object value) {
        if (!(value instanceof Number)) {
            return 0;
        }
        return Math.max(0, ((Number) value).intValue());
    }

    private static final class UpsertContext {
        private final List<String> fieldNames = new ArrayList<>();
        private final List<String> valuePlaceholders = new ArrayList<>();
        private final List<String> pkFieldNames = new ArrayList<>();
        private final List<String> updateSets = new ArrayList<>();
    }
}

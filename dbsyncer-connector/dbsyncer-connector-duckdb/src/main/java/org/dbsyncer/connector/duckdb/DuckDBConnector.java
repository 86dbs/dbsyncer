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

/**
 * DuckDB 连接器（本地文件库，可作为 Mapping 源/目标）。
 * <p>
 * 连接配置 {@code serviceName} 指向 .duckdb 文件。整库迁移相关能力暂不支持（与 H2 一致）。
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

    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        throw new DuckDBException("DuckDB 暂时不支持该功能");
    }

    @Override
    public boolean databaseExists(DatabaseConnectorInstance connectorInstance, String databaseName, String schemaName) {
        throw new DuckDBException("DuckDB 暂时不支持该功能");
    }

    @Override
    public String getTargetTableDDL(DatabaseConnectorInstance targetInstance, String tableName, String sourceDDL) {
        return "CREATE TABLE IF NOT EXISTS " + tableName + " (" + sourceDDL + ")";
    }

    @Override
    public String getSourceTableDDL(DatabaseConnectorInstance sourceInstance, String sourceTableName) {
        throw new DuckDBException("DuckDB 暂时不支持该功能");
    }

    @Override
    public String buildDropTableSql(DatabaseConnectorInstance targetInstance, String tableName) {
        throw new DuckDBException("Drop table is not supported.");
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
        throw new DuckDBException("DuckDB 暂时不支持该功能");
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

    private static final class UpsertContext {
        private final List<String> fieldNames = new ArrayList<>();
        private final List<String> valuePlaceholders = new ArrayList<>();
        private final List<String> pkFieldNames = new ArrayList<>();
        private final List<String> updateSets = new ArrayList<>();
    }
}

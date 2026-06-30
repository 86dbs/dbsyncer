/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.clickhouse;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.clickhouse.cdc.ClickHouseListener;
import org.dbsyncer.connector.clickhouse.schema.ClickHouseSchemaResolver;
import org.dbsyncer.connector.clickhouse.validator.ClickHouseConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
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
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ClickHouse 连接器实现
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:50
 */
public final class ClickHouseConnector extends AbstractDatabaseConnector {

    private static final Set<String> SYSTEM_DATABASES = Stream.of(
                    "system", "information_schema", "INFORMATION_SCHEMA", "_temporary_and_external_tables")
            .collect(Collectors.toSet());

    private static final String QUERY_TABLES = "SELECT name, engine FROM system.tables WHERE database = ? ORDER BY name";

    private static final String QUERY_COLUMNS = "SELECT name, type, numeric_precision, numeric_scale, is_in_primary_key, is_in_sorting_key "
            + "FROM system.columns WHERE database = ? AND table = ? ORDER BY position";

    private final ClickHouseConfigValidator configValidator = new ClickHouseConfigValidator();
    private final ClickHouseSchemaResolver schemaResolver = new ClickHouseSchemaResolver();

    @Override
    public String getConnectorType() {
        return "ClickHouse";
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
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        if (ListenerTypeEnum.isLog(listenerType)) {
            return new ClickHouseListener();
        }
        return null;
    }

    @Override
    protected boolean useJdbcTransaction() {
        return false;
    }

    @Override
    public ConnectorInstance connect(DatabaseConfig config, ConnectorServiceContext context) {
        String catalog = context.getCatalog();
        DatabaseConfig effectiveConfig = copyDatabaseConfig(config);
        effectiveConfig.setUrl(buildJdbcUrl(config, catalog));
        effectiveConfig.setDatabase(catalog);

        // 不支持显示setCatalog
        return new DatabaseConnectorInstance(effectiveConfig, StringUtil.EMPTY, context.getSchema());
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        StringBuilder url = new StringBuilder();
        url.append("jdbc:clickhouse://").append(config.getHost()).append(":").append(config.getPort());
        if (StringUtil.isNotBlank(database)) {
            url.append("/").append(database);
        }
        return url.toString();
    }

    @Override
    public String buildSqlWithQuotation() {
        return "`";
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> {
            List<String> databases = databaseTemplate.queryForList("SHOW DATABASES", String.class);
            if (CollectionUtils.isEmpty(databases)) {
                return Collections.emptyList();
            }
            return databases.stream()
                    .filter(name -> !SYSTEM_DATABASES.contains(name) && !SYSTEM_DATABASES.contains(name.toLowerCase(Locale.ROOT)))
                    .collect(Collectors.toList());
        });
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return Collections.emptyList();
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        String database = resolveDatabase(connectorInstance, context);
        if (StringUtil.isBlank(database)) {
            return Collections.emptyList();
        }
        return connectorInstance.execute(databaseTemplate -> {
            List<Map<String, Object>> rows = databaseTemplate.queryForList(QUERY_TABLES, database);
            if (CollectionUtils.isEmpty(rows)) {
                return Collections.emptyList();
            }
            List<Table> tables = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                Object nameValue = row.get("name");
                if (nameValue == null) {
                    continue;
                }
                Table table = new Table();
                table.setName(String.valueOf(nameValue));
                table.setType(resolveTableType(row.get("engine")));
                tables.add(table);
            }
            return tables;
        });
    }

    @Override
    protected String getSchema(String schema, Connection connection) {
        return null;
    }

    @Override
    public List<MetaInfo> getMetaInfo(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        if (CollectionUtils.isEmpty(context.getTablePatterns())) {
            return Collections.emptyList();
        }
        for (Table table : context.getTablePatterns()) {
            if (TableTypeEnum.getTableType(table.getType()) == getExtendedTableType()) {
                return super.getMetaInfo(connectorInstance, context);
            }
        }
        String database = resolveDatabase(connectorInstance, context);
        if (StringUtil.isBlank(database)) {
            return Collections.emptyList();
        }
        return connectorInstance.execute(databaseTemplate -> {
            List<MetaInfo> metaInfos = new ArrayList<>();
            for (Table table : context.getTablePatterns()) {
                String tableName = table.getName();
                List<Map<String, Object>> rows = databaseTemplate.queryForList(QUERY_COLUMNS, database, tableName);
                if (CollectionUtils.isEmpty(rows)) {
                    continue;
                }
                List<String> primaryKeys = new ArrayList<>();
                List<String> sortingKeys = new ArrayList<>();
                List<Field> fields = new ArrayList<>(rows.size());
                for (Map<String, Object> row : rows) {
                    Object nameValue = row.get("name");
                    if (nameValue == null) {
                        continue;
                    }
                    String columnName = String.valueOf(nameValue);
                    if (isTruthy(row.get("is_in_primary_key"))) {
                        primaryKeys.add(columnName);
                    }
                    if (isTruthy(row.get("is_in_sorting_key"))) {
                        sortingKeys.add(columnName);
                    }
                }
                List<String> effectivePrimaryKeys = !primaryKeys.isEmpty() ? primaryKeys : sortingKeys;
                for (Map<String, Object> row : rows) {
                    Object nameValue = row.get("name");
                    if (nameValue == null) {
                        continue;
                    }
                    String columnName = String.valueOf(nameValue);
                    Object typeValue = row.get("type");
                    String typeName = typeValue == null ? StringUtil.EMPTY : String.valueOf(typeValue);
                    boolean pk = effectivePrimaryKeys.stream().anyMatch(key -> key.equalsIgnoreCase(columnName));
                    fields.add(new Field(columnName, typeName, Types.OTHER, pk,
                            toNonNegativeInt(row.get("numeric_precision")), toNonNegativeInt(row.get("numeric_scale"))));
                }
                MetaInfo metaInfo = new MetaInfo();
                metaInfo.setTable(tableName);
                metaInfo.setTableType(StringUtil.isNotBlank(table.getType()) ? table.getType() : TableTypeEnum.TABLE.getCode());
                metaInfo.setColumn(fields);
                metaInfos.add(metaInfo);
            }
            return metaInfos;
        });
    }

    @Override
    public boolean databaseExists(DatabaseConnectorInstance connectorInstance, String databaseName, String schemaName) {
        if (StringUtil.isBlank(databaseName)) {
            return false;
        }
        Integer count = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject("SELECT count() FROM system.databases WHERE name = ?", Integer.class, databaseName));
        return count != null && count > 0;
    }

    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        if (StringUtil.isBlank(databaseName)) {
            return StringUtil.EMPTY;
        }
        return "CREATE DATABASE IF NOT EXISTS " + buildWithQuotation(databaseName);
    }

    @Override
    public String getTargetTableDDL(DatabaseConnectorInstance targetInstance, String tableName, String sourceDDL) {
        return "CREATE TABLE IF NOT EXISTS " + qualifyTableName(targetInstance, tableName)
                + " (" + sourceDDL + ") ENGINE = MergeTree() ORDER BY tuple()";
    }

    @Override
    public String getSourceTableDDL(DatabaseConnectorInstance sourceInstance, String sourceTableName) {
        if (sourceInstance == null || StringUtil.isBlank(sourceTableName)) {
            return StringUtil.EMPTY;
        }
        return sourceInstance.execute(databaseTemplate -> {
            List<java.util.Map<String, Object>> rows = databaseTemplate.queryForList("SHOW CREATE TABLE " + sourceTableName);
            if (CollectionUtils.isEmpty(rows)) {
                return StringUtil.EMPTY;
            }
            Object ddl = rows.get(0).get("statement");
            return ddl == null ? StringUtil.EMPTY : String.valueOf(ddl);
        });
    }

    @Override
    public String buildDropTableSql(DatabaseConnectorInstance targetInstance, String tableName) {
        if (StringUtil.isBlank(tableName)) {
            return StringUtil.EMPTY;
        }
        return "DROP TABLE IF EXISTS " + qualifyTableName(targetInstance, tableName);
    }

    private String qualifyTableName(DatabaseConnectorInstance targetInstance, String tableName) {
        String qualifiedTable = buildWithQuotation(tableName);
        if (targetInstance != null && StringUtil.isNotBlank(targetInstance.getCatalog())) {
            return buildWithQuotation(targetInstance.getCatalog()) + "." + qualifiedTable;
        }
        return qualifiedTable;
    }

    @Override
    public String getPageSql(PageSql config) {
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        appendOrderByPrimaryKeys(sql, config);
        sql.append(DatabaseConstant.CLICKHOUSE_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public String getPageCursorSql(PageSql config) {

        StringBuilder sql = new StringBuilder(config.getQuerySql());
        buildCursorConditionAndOrderBy(sql, config);
        sql.append(DatabaseConstant.CLICKHOUSE_PAGE_SQL);
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
        if (CollectionUtils.isEmpty(sourceDefinitions) || CollectionUtils.isEmpty(targetColumnNames)) {
            return StringUtil.EMPTY;
        }
        int loopSize = Math.min(sourceDefinitions.size(), targetColumnNames.size());
        String qualifiedTable = buildWithQuotation(targetTableName);
        List<String> clauses = new ArrayList<>(loopSize);
        for (int i = 0; i < loopSize; i++) {
            Field sourceField = sourceDefinitions.get(i);
            String targetColumn = targetColumnNames.get(i);
            if (sourceField == null || StringUtil.isBlank(targetColumn)) {
                continue;
            }
            String col = buildWithQuotation(targetColumn);
            String type = formatPhysicalType(sourceField);
            clauses.add(String.format(Locale.ROOT, "MODIFY COLUMN %s %s", col, type));
        }
        if (clauses.isEmpty()) {
            return StringUtil.EMPTY;
        }
        return String.format(Locale.ROOT, "ALTER TABLE %s %s", qualifiedTable, StringUtil.join(clauses, ", "));
    }

    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        UpsertContext context = buildUpsertContext(config);
        return config.getDatabase().generateUniqueCode() + "INSERT INTO " + config.getSchema()
                + config.getDatabase().buildWithQuotation(config.getTableName()) + "("
                + StringUtil.join(context.fieldNames, StringUtil.COMMA) + ") VALUES ("
                + StringUtil.join(context.valuePlaceholders, StringUtil.COMMA) + ")";
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        return buildInsertSql(config);
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
            }
        });
        return context;
    }

    private String resolveDatabase(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        if (context != null && StringUtil.isNotBlank(context.getCatalog())) {
            return context.getCatalog().trim();
        }
        if (StringUtil.isNotBlank(connectorInstance.getCatalog())) {
            return connectorInstance.getCatalog().trim();
        }
        return connectorInstance.execute(databaseTemplate -> {
            try {
                Connection connection = databaseTemplate.getSimpleConnection().getConnection();
                String catalog = connection.getCatalog();
                return StringUtil.isNotBlank(catalog) ? catalog.trim() : StringUtil.EMPTY;
            } catch (SQLException e) {
                return StringUtil.EMPTY;
            }
        });
    }

    private String resolveTableType(Object engine) {
        if (engine == null) {
            return TableTypeEnum.TABLE.getCode();
        }
        String engineName = String.valueOf(engine);
        if ("View".equalsIgnoreCase(engineName)) {
            return TableTypeEnum.VIEW.getCode();
        }
        if ("MaterializedView".equalsIgnoreCase(engineName)) {
            return TableTypeEnum.MATERIALIZED_VIEW.getCode();
        }
        return TableTypeEnum.TABLE.getCode();
    }

    private boolean isTruthy(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return "1".equals(String.valueOf(value));
    }

    private int toNonNegativeInt(Object value) {
        if (!(value instanceof Number)) {
            return 0;
        }
        return Math.max(0, ((Number) value).intValue());
    }

    private DatabaseConfig copyDatabaseConfig(DatabaseConfig source) {
        DatabaseConfig target = new DatabaseConfig();
        target.setConnectorType(source.getConnectorType());
        target.setDriverClassName(source.getDriverClassName());
        target.setHost(source.getHost());
        target.setPort(source.getPort());
        target.setUsername(source.getUsername());
        target.setPassword(source.getPassword());
        target.setMaxActive(source.getMaxActive());
        target.setKeepAlive(source.getKeepAlive());
        target.setDatabase(source.getDatabase());
        target.setServiceName(source.getServiceName());
        target.setUrl(source.getUrl());
        if (source.getProperties() != null) {
            Properties properties = new Properties();
            properties.putAll(source.getProperties());
            target.setProperties(properties);
        }
        if (source.getExtInfo() != null) {
            Properties extInfo = new Properties();
            extInfo.putAll(source.getExtInfo());
            target.setExtInfo(extInfo);
        }
        return target;
    }

    private static class UpsertContext {
        List<String> fieldNames = new ArrayList<>();
        List<String> valuePlaceholders = new ArrayList<>();
        List<String> pkFieldNames = new ArrayList<>();
    }
}

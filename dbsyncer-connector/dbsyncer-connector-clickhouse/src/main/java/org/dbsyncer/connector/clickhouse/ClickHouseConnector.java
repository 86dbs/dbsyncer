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
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
    public String buildCreateTableSql(String tableName, String tableBodySql, boolean ifNotExists) {
        if (ifNotExists) {
            return "CREATE TABLE IF NOT EXISTS " + tableName + " (" + tableBodySql + ") ENGINE = MergeTree() ORDER BY tuple()";
        }
        return "CREATE TABLE " + tableName + " (" + tableBodySql + ") ENGINE = MergeTree() ORDER BY tuple()";
    }

    @Override
    public String buildDropTableSql(String tableName, boolean ifExists) {
        String quoted = buildWithQuotation(tableName);
        if (ifExists) {
            return "DROP TABLE IF EXISTS " + quoted;
        }
        return "DROP TABLE " + quoted;
    }

    @Override
    public String getCreateTableDdl(DatabaseConnectorInstance connectorInstance, String tableName, boolean ifNotExists) {
        if (connectorInstance == null || StringUtil.isBlank(tableName)) {
            return StringUtil.EMPTY;
        }
        return connectorInstance.execute(databaseTemplate -> {
            List<java.util.Map<String, Object>> rows = databaseTemplate.queryForList("SHOW CREATE TABLE " + tableName);
            if (CollectionUtils.isEmpty(rows)) {
                return StringUtil.EMPTY;
            }
            Object ddl = rows.get(0).get("statement");
            return ddl == null ? StringUtil.EMPTY : String.valueOf(ddl);
        });
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
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }
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

    private static class UpsertContext {
        List<String> fieldNames = new ArrayList<>();
        List<String> valuePlaceholders = new ArrayList<>();
        List<String> pkFieldNames = new ArrayList<>();
    }
}

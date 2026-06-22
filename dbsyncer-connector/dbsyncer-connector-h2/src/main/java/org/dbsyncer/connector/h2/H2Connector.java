/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.h2;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.h2.schema.H2SchemaResolver;
import org.dbsyncer.connector.h2.storage.H2StorageService;
import org.dbsyncer.connector.h2.validator.H2ConfigValidator;
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
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * H2 内嵌数据库连接器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2021-11-22 23:55
 */
public final class H2Connector extends AbstractDatabaseConnector {

    private final String QUERY_SCHEMA = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')";

    private final H2ConfigValidator configValidator = new H2ConfigValidator();
    private final H2SchemaResolver schemaResolver = new H2SchemaResolver();

    @Override
    public String getConnectorType() {
        return "H2";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:h2:file:./data/h2db
        return "jdbc:h2:file:" + database;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        return null;
    }

    @Override
    public StorageService getStorageService() {
        return new H2StorageService();
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> {
            try {
                String name = databaseTemplate.queryForObject("SELECT DATABASE()", String.class);
                if (StringUtil.isNotBlank(name)) {
                    return Collections.singletonList(name);
                }
            } catch (Exception ignored) {
                // fallback below
            }
            return Collections.singletonList("PUBLIC");
        });
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_SCHEMA, String.class));
    }

    @Override
    public String buildSqlWithQuotation() {
        return "`";
    }

    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        throw new H2Exception("H2 暂时不支持该功能");

    }

    @Override
    public boolean databaseExists(DatabaseConnectorInstance connectorInstance, String databaseName, String schemaName) {
        throw new H2Exception("H2 暂时不支持该功能");
    }

    @Override
    public String buildCreateTableSql(DatabaseConnectorInstance targetInstance, String tableName, String tableBodySql) {
        return "CREATE TABLE IF NOT EXISTS " + tableName + " (" + tableBodySql + ")";
    }

    @Override
    public String getCreateTableDdl(DatabaseConnectorInstance sourceInstance, DatabaseConnectorInstance targetInstance,
                                    String sourceTableName, String targetTableName) {
        throw new H2Exception("H2 暂时不支持该功能");
    }

    @Override
    public String buildDropTableSql(DatabaseConnectorInstance targetInstance, String tableName) {
        throw new H2Exception("Drop table is not supported.");
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
    public String buildModifyColumnsSql(DatabaseConnectorInstance targetInstance, ValidateSyncTask task, String targetTableName,
                                        List<Field> sourceDefinitions, List<String> targetColumnNames) {
        throw new H2Exception("H2 暂时不支持该功能");
    }

    @Override
    public String generateUniqueCode() {
        return DatabaseConstant.DBS_UNIQUE_CODE;
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<String> fieldNames = new ArrayList<>();
        List<String> valuePlaceholders = new ArrayList<>();
        List<String> duplicateUpdates = new ArrayList<>();
        config.getFields().forEach(f -> {
            String name = database.buildWithQuotation(f.getName());
            fieldNames.add(name);
            valuePlaceholders.add("?");
            if (!f.isPk()) {
                duplicateUpdates.add(String.format("%s = VALUES(%s)", name, name));
            }
        });

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = buildTableName(config);
        return String.format("%sINSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;", uniqueCode, table,
                StringUtil.join(fieldNames, StringUtil.COMMA), StringUtil.join(valuePlaceholders, StringUtil.COMMA),
                StringUtil.join(duplicateUpdates, StringUtil.COMMA));
    }

    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<String> fieldNames = new ArrayList<>();
        List<String> valuePlaceholders = new ArrayList<>();
        config.getFields().forEach(f -> {
            fieldNames.add(database.buildWithQuotation(f.getName()));
            valuePlaceholders.add("?");
        });

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = buildTableName(config);
        return String.format("%sINSERT IGNORE INTO %s (%s) VALUES (%s)", uniqueCode, table,
                StringUtil.join(fieldNames, StringUtil.COMMA), StringUtil.join(valuePlaceholders, StringUtil.COMMA));
    }

    private StringBuilder buildTableName(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        StringBuilder table = new StringBuilder();
        table.append(config.getSchema());
        table.append(database.buildWithQuotation(config.getTableName()));
        return table;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }
}

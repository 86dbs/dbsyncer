/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.doris;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.doris.constant.DorisConstant;
import org.dbsyncer.connector.doris.load.DorisStreamLoadWriter;
import org.dbsyncer.connector.doris.schema.DorisSchemaResolver;
import org.dbsyncer.connector.doris.validator.DorisConfigValidator;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Apache Doris 连接器（兼容 MySQL 协议）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 21:00
 */
public final class DorisConnector extends MySQLConnector {

    private static final Set<String> SYSTEM_DATABASES = Stream.of(
            "information_schema", "mysql", "__internal_schema")
            .collect(Collectors.toSet());

    /**
     * 匹配列定义末尾的 MySQL 风格 PRIMARY KEY 子句，Doris 建表前需剥离。
     */
    private static final Pattern PRIMARY_KEY_CLAUSE = Pattern.compile(
            ",?\\s*PRIMARY\\s+KEY\\s*\\(([^)]+)\\)\\s*$", Pattern.CASE_INSENSITIVE);

    private final DorisConfigValidator configValidator = new DorisConfigValidator();
    private final DorisSchemaResolver schemaResolver = new DorisSchemaResolver();
    private final DorisStreamLoadWriter streamLoadWriter = new DorisStreamLoadWriter();

    @Override
    public String getConnectorType() {
        return "Doris";
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
    public ConnectorInstance connect(DatabaseConfig config, ConnectorServiceContext context) {
        DorisConstant.enrichJdbcProperties(config);
        return super.connect(config, context);
    }

    @Override
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        DatabaseConfig config = connectorInstance.getConfig();
        if (DorisConstant.isStreamLoadMode(config) && DorisConstant.isStreamLoadEvent(context.getEvent())) {
            return streamLoadWriter.write(connectorInstance, context);
        }
        return super.writer(connectorInstance, context);
    }

    /**
     * Doris 不支持 MySQL 的 ON DUPLICATE KEY UPDATE；Unique/Primary Key 模型下普通 INSERT 即按键覆盖。
     */
    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        return buildPlainInsertSql(config);
    }

    /**
     * Doris 不支持 INSERT IGNORE，使用普通 INSERT。
     */
    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        return buildPlainInsertSql(config);
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
        return null;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> {
            List<String> databases = databaseTemplate.queryForList("SHOW DATABASES", String.class);
            if (CollectionUtils.isEmpty(databases)) {
                return Collections.emptyList();
            }
            return databases.stream()
                    .filter(name -> !SYSTEM_DATABASES.contains(name.toLowerCase()))
                    .collect(Collectors.toList());
        });
    }

    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        if (StringUtil.isBlank(databaseName)) {
            return StringUtil.EMPTY;
        }
        return "CREATE DATABASE IF NOT EXISTS " + buildWithQuotation(databaseName);
    }

    @Override
    public boolean databaseExists(DatabaseConnectorInstance connectorInstance, String databaseName, String schemaName) {
        if (StringUtil.isBlank(databaseName)) {
            return false;
        }
        return connectorInstance.execute(databaseTemplate ->
                !CollectionUtils.isEmpty(databaseTemplate.queryForList("SHOW DATABASES LIKE ?", String.class, databaseName)));
    }

    /**
     * Doris 建表必须指定 KEY 模型与分桶。跨库迁移默认使用明细模型（DUPLICATE KEY），
     * 以首列为排序/分桶键，避免 UNIQUE KEY 要求键列为 schema 有序前缀导致建表失败。
     */
    @Override
    public String getTargetTableDDL(DatabaseConnectorInstance targetInstance, String tableName, String sourceDDL) {
        if (StringUtil.isBlank(sourceDDL)) {
            return StringUtil.EMPTY;
        }
        String columns = stripPrimaryKeyClause(sourceDDL.trim());
        String firstColumn = extractFirstColumnName(columns);
        if (StringUtil.isBlank(firstColumn)) {
            return StringUtil.EMPTY;
        }
        String qualifiedTable = qualifyTableName(targetInstance, tableName);
        String keyCol = buildWithQuotation(firstColumn);
        return String.format(Locale.ROOT,
                "CREATE TABLE IF NOT EXISTS %s (%s) DUPLICATE KEY(%s) DISTRIBUTED BY HASH(%s) BUCKETS 10 "
                        + "PROPERTIES (\"replication_num\" = \"1\")",
                qualifiedTable, columns, keyCol, keyCol);
    }

    private static String stripPrimaryKeyClause(String sourceDDL) {
        Matcher pkMatcher = PRIMARY_KEY_CLAUSE.matcher(sourceDDL);
        if (!pkMatcher.find()) {
            return sourceDDL;
        }
        String columns = pkMatcher.replaceFirst(StringUtil.EMPTY).trim();
        if (columns.endsWith(StringUtil.COMMA)) {
            columns = columns.substring(0, columns.length() - 1).trim();
        }
        return columns;
    }

    @Override
    public String getSourceTableDDL(DatabaseConnectorInstance sourceInstance, String sourceTableName) {
        // Doris 兼容 MySQL 协议，SHOW CREATE TABLE 返回格式与 MySQL 一致
        return super.getSourceTableDDL(sourceInstance, sourceTableName);
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
        if (targetInstance == null) {
            return qualifiedTable;
        }
        String database = StringUtil.isNotBlank(targetInstance.getCatalog())
                ? targetInstance.getCatalog()
                : targetInstance.getSchema();
        if (StringUtil.isNotBlank(database)) {
            return buildWithQuotation(database) + "." + qualifiedTable;
        }
        return qualifiedTable;
    }

    private static String extractFirstColumnName(String columns) {
        if (StringUtil.isBlank(columns)) {
            return StringUtil.EMPTY;
        }
        String first = columns.split(StringUtil.COMMA, 2)[0].trim();
        if (StringUtil.isBlank(first)) {
            return StringUtil.EMPTY;
        }
        String[] tokens = first.split("\\s+");
        return unwrapIdentifier(tokens[0]);
    }

    private static String unwrapIdentifier(String identifier) {
        if (StringUtil.isBlank(identifier)) {
            return StringUtil.EMPTY;
        }
        String name = identifier.trim();
        if ((name.startsWith("`") && name.endsWith("`"))
                || (name.startsWith("\"") && name.endsWith("\""))
                || (name.startsWith("[") && name.endsWith("]"))) {
            return name.substring(1, name.length() - 1);
        }
        return name;
    }

    private String buildPlainInsertSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();
        List<String> fieldNames = new ArrayList<>(fields.size());
        List<String> placeholders = new ArrayList<>(fields.size());
        for (Field field : fields) {
            fieldNames.add(database.buildWithQuotation(field.getName()));
            placeholders.add("?");
        }
        StringBuilder table = new StringBuilder();
        table.append(config.getSchema());
        table.append(database.buildWithQuotation(config.getTableName()));
        return String.format("%sINSERT INTO %s (%s) VALUES (%s)",
                database.generateUniqueCode(),
                table,
                StringUtil.join(fieldNames, StringUtil.COMMA),
                StringUtil.join(placeholders, StringUtil.COMMA));
    }
}

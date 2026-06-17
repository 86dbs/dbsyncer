/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.postgresql.cdc.PostgreSQLListener;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLSchemaResolver;
import org.dbsyncer.connector.postgresql.validator.PostgreSQLConfigValidator;
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
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * PostgreSQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    private final String QUERY_DATABASE = "SELECT datname FROM pg_database WHERE datistemplate = FALSE order by datname";
    private final String QUERY_SCHEMA = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name NOT IN ('information_schema') ORDER BY schema_name";

    private final PostgreSQLConfigValidator configValidator = new PostgreSQLConfigValidator();
    private final PostgreSQLSchemaResolver schemaResolver = new PostgreSQLSchemaResolver();

    @Override
    public String getConnectorType() {
        return "PostgreSQL";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new PostgreSQLListener();
        }
        return null;
    }

    @Override
    public ConnectorInstance connect(DatabaseConfig config, ConnectorServiceContext context) {
        String catalog = context.getCatalog();
        if (StringUtil.isNotBlank(catalog)) {
            DatabaseConfig effectiveConfig = copyDatabaseConfig(config);
            effectiveConfig.setUrl(buildJdbcUrl(config, catalog));
            effectiveConfig.setDatabase(catalog);
            return new DatabaseConnectorInstance(effectiveConfig, catalog, context.getSchema());
        }
        return super.connect(config, context);
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_DATABASE, String.class));
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        if (StringUtil.isBlank(catalog)) {
            return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_SCHEMA, String.class));
        }
        DatabaseConfig effectiveConfig = copyDatabaseConfig(connectorInstance.getConfig());
        effectiveConfig.setUrl(buildJdbcUrl(connectorInstance.getConfig(), catalog));
        effectiveConfig.setDatabase(catalog);
        DatabaseConnectorInstance catalogInstance = new DatabaseConnectorInstance(effectiveConfig, catalog, null);
        try {
            return catalogInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_SCHEMA, String.class));
        } finally {
            catalogInstance.close();
        }
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        if (StringUtil.isBlank(databaseName)) {
            return StringUtil.EMPTY;
        }
        return "CREATE DATABASE " + buildWithQuotation(databaseName);
    }

    @Override
    public boolean databaseExists(DatabaseConnectorInstance connectorInstance, String databaseName, String schemaName) {
        if (StringUtil.isBlank(databaseName)) {
            return false;
        }
        Integer count = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject("SELECT COUNT(1) FROM pg_database WHERE datname = ?", Integer.class, databaseName));
        return count != null && count > 0;
    }

    @Override
    public String buildCreateTableSql(String tableName, String tableBodySql) {
        String qualifiedTable = qualifyTableName(null, tableName);
        return "CREATE TABLE IF NOT EXISTS " + qualifiedTable + " (" + tableBodySql + ")";
    }

    @Override
    public String buildDropTableSql(String tableName, boolean ifExists) {
        String qualifiedTable = qualifyTableName(null, tableName);
        if (ifExists) {
            return "DROP TABLE IF EXISTS " + qualifiedTable + " CASCADE";
        }
        return "DROP TABLE " + qualifiedTable + " CASCADE";
    }

    String qualifyTableName(String schema, String tableName) {
        String table = buildWithQuotation(tableName);
        if (StringUtil.isBlank(schema)) {
            return table;
        }
        return buildWithQuotation(schema) + "." + table;
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

    @Override
    public String getCreateTableDdl(DatabaseConnectorInstance connectorInstance, String tableName) {
        return null;
    }

    @Override
    public String getPageSql(PageSql config) {
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类方法添加ORDER BY（按主键排序，保证分页一致性）
        appendOrderByPrimaryKeys(sql, config);
        sql.append(DatabaseConstant.POSTGRESQL_PAGE_SQL);
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
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }

        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类的公共方法构建WHERE条件和ORDER BY
        buildCursorConditionAndOrderBy(sql, config);
        sql.append(DatabaseConstant.POSTGRESQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors || cursors.length == 0) {
            return new Object[]{pageSize, 0};
        }
        // 使用基类的公共方法构建游标条件参数
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{pageSize, 0};
        }

        // PostgreSQL使用 LIMIT ? OFFSET ?，参数顺序为 [游标参数..., pageSize, 0]
        Object[] newCursors = new Object[cursorArgs.length + 2];
        System.arraycopy(cursorArgs, 0, newCursors, 0, cursorArgs.length);
        newCursors[cursorArgs.length] = pageSize; // LIMIT
        newCursors[cursorArgs.length + 1] = 0; // OFFSET
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
        String qualifiedTable = qualifyTable(task, targetTableName);
        List<String> clauses = new ArrayList<>(loopSize);
        for (int i = 0; i < loopSize; i++) {
            Field sourceField = sourceDefinitions.get(i);
            String targetColumn = targetColumnNames.get(i);
            if (sourceField == null || StringUtil.isBlank(targetColumn)) {
                continue;
            }
            String col = buildWithQuotation(targetColumn);
            String type = formatPhysicalType(sourceField);
            String usingExpr = buildUsingExpr(col, type);
            clauses.add(String.format(Locale.ROOT, "ALTER COLUMN %s TYPE %s USING %s", col, type, usingExpr));
        }
        if (clauses.isEmpty()) {
            return StringUtil.EMPTY;
        }
        return String.format(Locale.ROOT, "ALTER TABLE %s %s", qualifiedTable, StringUtil.join(clauses, ", "));
    }

    private String qualifyTable(ValidateSyncTask task, String tableName) {
        String schema = StringUtil.isNotBlank(task.getTargetSchema()) ? task.getTargetSchema() : "public";
        return buildWithQuotation(schema) + "." + buildWithQuotation(tableName);
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    protected String getSchema(String schema, Connection connection) {
        return StringUtil.isNotBlank(schema) ? schema : "public";
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:postgresql://127.0.0.1:5432/postgres
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://").append(config.getHost()).append(":").append(config.getPort()).append("/");
        if (StringUtil.isNotBlank(database)) {
            url.append(database);
        }
        return url.toString();
    }

    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        // PostgreSQL 使用 ON CONFLICT DO NOTHING 实现 INSERT IGNORE 效果
        UpsertContext context = buildUpsertContext(config);
        StringBuilder sql = new StringBuilder(config.getDatabase().generateUniqueCode());

        // 构建 INSERT INTO ... VALUES (...)
        buildInsertIntoClause(sql, config, context);

        // 构建 ON CONFLICT (...) DO NOTHING
        buildOnConflictClause(sql, context);
        sql.append(" DO NOTHING");

        return sql.toString();
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        UpsertContext context = buildUpsertContext(config);
        StringBuilder sql = new StringBuilder(config.getDatabase().generateUniqueCode());

        // 构建 INSERT INTO ... VALUES (...)
        buildInsertIntoClause(sql, config, context);

        // 构建 ON CONFLICT (...) DO UPDATE SET
        buildOnConflictClause(sql, context);
        sql.append(" DO UPDATE SET ");
        sql.append(StringUtil.join(context.updateSets, StringUtil.COMMA));

        return sql.toString();
    }

    /**
     * 构建 INSERT INTO ... VALUES (...) 子句
     */
    private void buildInsertIntoClause(StringBuilder sql, SqlBuilderConfig config, UpsertContext context) {
        sql.append("INSERT INTO ").append(config.getSchema());
        sql.append(config.getDatabase().buildWithQuotation(config.getTableName()));
        sql.append("(").append(StringUtil.join(context.fieldNames, StringUtil.COMMA)).append(") ");
        sql.append("OVERRIDING SYSTEM VALUE VALUES (").append(StringUtil.join(context.valuePlaceholders, StringUtil.COMMA)).append(")");
    }

    /**
     * 构建 ON CONFLICT (...) 子句
     */
    private void buildOnConflictClause(StringBuilder sql, UpsertContext context) {
        sql.append(" ON CONFLICT (");
        sql.append(StringUtil.join(context.pkFieldNames, StringUtil.COMMA));
        sql.append(")");
    }

    private String buildUsingExpr(String col, String type) {
        String normalizedType = StringUtil.trim(type).toUpperCase(Locale.ROOT);
        if (isIntegerType(normalizedType)) {
            // 非整数字符串置为NULL，避免 timestamp/text -> int 直接报错中断整批DDL
            return String.format(Locale.ROOT,
                    "CASE WHEN %1$s IS NULL THEN NULL WHEN (%1$s)::text ~ '^-?[0-9]+$' THEN (%1$s)::text::%2$s ELSE NULL END",
                    col, type);
        }
        if (isDecimalType(normalizedType)) {
            // 小数目标类型同样做安全转换，无法解析时置为NULL
            return String.format(Locale.ROOT,
                    "CASE WHEN %1$s IS NULL THEN NULL WHEN (%1$s)::text ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (%1$s)::text::%2$s ELSE NULL END",
                    col, type);
        }
        return String.format(Locale.ROOT, "%s::text::%s", col, type);
    }

    private boolean isIntegerType(String type) {
        return "INT2".equals(type) || "INT4".equals(type) || "INT8".equals(type)
                || "SMALLINT".equals(type) || "INTEGER".equals(type) || "BIGINT".equals(type);
    }

    private boolean isDecimalType(String type) {
        return "REAL".equals(type) || "DOUBLE PRECISION".equals(type)
                || type.startsWith("NUMERIC") || type.startsWith("DECIMAL");
    }

    @Override
    protected String formatPhysicalType(Field sourceDefinition) {
        if (sourceDefinition == null || StringUtil.isBlank(sourceDefinition.getTypeName())) {
            return super.formatPhysicalType(sourceDefinition);
        }
        String t = sourceDefinition.getTypeName().trim().toUpperCase(Locale.ROOT);
        String serialMapped = mapPostgreSqlSerialToInteger(t);
        if (serialMapped != null) {
            return serialMapped;
        }
        String floatMapped = mapPostgreSqlFloatAliases(t);
        if (floatMapped != null) {
            return floatMapped;
        }
        return super.formatPhysicalType(sourceDefinition);
    }

    /**
     * ALTER TYPE 不能使用 SERIAL 伪类型，映射为底层整数类型。
     */
    private static String mapPostgreSqlSerialToInteger(String t) {
        if ("SERIAL".equals(t)) {
            return "INT4";
        }
        if ("BIGSERIAL".equals(t)) {
            return "INT8";
        }
        if ("SMALLSERIAL".equals(t)) {
            return "INT2";
        }
        if ("BYTEA".equals(t)) {
            return "BYTEA";
        }
        String rawTypeName = PostgreSQLSchemaResolver.normalizeTypeName(t);
        if (PostgreSQLSchemaResolver.isArrayType(rawTypeName)) {
            return t;
        }
        return null;
    }

    /**
     * FLOAT4/8 等与基类带精度拼接冲突时，改为 DDL 中的规范名称。
     */
    private static String mapPostgreSqlFloatAliases(String t) {
        if ("FLOAT4".equals(t) || "REAL".equals(t)) {
            return "REAL";
        }
        if ("FLOAT8".equals(t) || "DOUBLE PRECISION".equals(t)) {
            return "DOUBLE PRECISION";
        }
        return null;
    }

    /**
     * 构建 UPSERT 上下文（字段、主键等信息）
     */
    private UpsertContext buildUpsertContext(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        UpsertContext context = new UpsertContext();

        config.getFields().forEach(f -> {
            String fieldName = database.buildWithQuotation(f.getName());
            context.fieldNames.add(fieldName);

            // 构建 VALUES 占位符
            List<String> fieldVs = new ArrayList<>();
            if (database.buildCustomValue(fieldVs, f)) {
                // 自定义值表达式（如 geometry 类型）
                context.valuePlaceholders.add(fieldVs.get(0));
            } else {
                context.valuePlaceholders.add("?");
            }

            if (f.isPk()) {
                context.pkFieldNames.add(fieldName);
            } else {
                // UPDATE SET fieldName = EXCLUDED.fieldName
                context.updateSets.add(String.format("%s = EXCLUDED.%s", fieldName, fieldName));
            }
        });

        return context;
    }

    /**
     * UPSERT 语句构建上下文
     */
    private static class UpsertContext {

        List<String> fieldNames = new ArrayList<>();
        List<String> valuePlaceholders = new ArrayList<>();
        List<String> pkFieldNames = new ArrayList<>();
        List<String> updateSets = new ArrayList<>();
    }

}
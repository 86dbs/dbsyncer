/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.postgresql.cdc.PostgreSQLListener;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLSchemaResolver;
import org.dbsyncer.connector.postgresql.validator.PostgreSQLConfigValidator;
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
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    private final String QUERY_DATABASE = "SELECT datname FROM pg_database WHERE datistemplate = FALSE order by datname";
    private final String QUERY_SCHEMA = "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '#' and schema_name NOT LIKE 'pg_%' AND schema_name not in('information_schema') order by schema_name";

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
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate->databaseTemplate.queryForList(QUERY_DATABASE, String.class));
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return connectorInstance.execute(databaseTemplate->databaseTemplate.queryForList(QUERY_SCHEMA.replace("#", catalog), String.class));
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
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
        sql.append("VALUES (").append(StringUtil.join(context.valuePlaceholders, StringUtil.COMMA)).append(")");
    }

    /**
     * 构建 ON CONFLICT (...) 子句
     */
    private void buildOnConflictClause(StringBuilder sql, UpsertContext context) {
        sql.append(" ON CONFLICT (");
        sql.append(StringUtil.join(context.pkFieldNames, StringUtil.COMMA));
        sql.append(")");
    }

    /**
     * 构建 UPSERT 上下文（字段、主键等信息）
     */
    private UpsertContext buildUpsertContext(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        UpsertContext context = new UpsertContext();

        config.getFields().forEach(f-> {
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
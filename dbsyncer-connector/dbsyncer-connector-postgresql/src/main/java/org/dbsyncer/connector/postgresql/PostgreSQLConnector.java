/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.postgresql.cdc.PostgreSQLListener;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLSchemaResolver;
import org.dbsyncer.connector.postgresql.validator.PostgreSQLConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

/**
 * PostgreSQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

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
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_DATABASE, String.class));
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_SCHEMA.replace("#", catalog), String.class));
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
        newCursors[cursorArgs.length] = pageSize;  // LIMIT
        newCursors[cursorArgs.length + 1] = 0;  // OFFSET
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

}
/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.sqlite.schema.SQLiteSchemaResolver;
import org.dbsyncer.connector.sqlite.validator.SQLiteConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQLite连接器实现
 * @Author bble
 * @Version 1.0.0
 * @Date 2023-11-28 16:22
 */
public final class SQLiteConnector extends AbstractDatabaseConnector {

    private final String QUERY_VIEW = "SELECT name FROM sqlite_master WHERE type = 'view'";
    private final String QUERY_TABLE = "SELECT name FROM sqlite_master WHERE type='table'";
    private final String QUERY_DATABASE = "PRAGMA database_list";

    private final SQLiteConfigValidator configValidator = new SQLiteConfigValidator();
    private final SQLiteSchemaResolver schemaResolver = new SQLiteSchemaResolver();

    @Override
    public String getConnectorType() {
        return "SQLite";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        List<Table> tables = getTables(connectorInstance, String.format(QUERY_TABLE, context.getSchema()), TableTypeEnum.TABLE);
        tables.addAll(getTables(connectorInstance, QUERY_VIEW, TableTypeEnum.VIEW));
        return tables;
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:sqlite:C:/Users/example.db
        return "jdbc:sqlite:" + database;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        return null;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate-> {
            Map<String, Object> result = databaseTemplate.queryForMap(QUERY_DATABASE);
            List<String> list = new ArrayList<>();
            if (!CollectionUtils.isEmpty(result)) {
                list.add(String.valueOf(result.get("name")));
            }
            return list;
        });
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
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }

        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类的公共方法构建WHERE条件和ORDER BY
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
        // 使用基类的公共方法构建游标条件参数
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{pageSize, 0};
        }

        // SQLite使用 LIMIT ? OFFSET ?，参数顺序为 [游标参数..., pageSize, 0]
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
    public String buildInsertSql(SqlBuilderConfig config) {
        // SQLite 使用 INSERT OR IGNORE 实现 INSERT IGNORE 效果
        UpsertContext context = buildUpsertContext(config);

        // 构建 INSERT OR IGNORE INTO ... VALUES (...)
        return config.getDatabase().generateUniqueCode() + "INSERT OR IGNORE INTO " + config.getSchema() + config.getDatabase().buildWithQuotation(config.getTableName()) + "("
                + StringUtil.join(context.fieldNames, StringUtil.COMMA) + ") " + "VALUES (" + StringUtil.join(context.valuePlaceholders, StringUtil.COMMA) + ")";
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        // SQLite 3.24.0+ 支持 ON CONFLICT DO UPDATE 语法
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
                // 自定义值表达式（如特殊类型）
                context.valuePlaceholders.add(fieldVs.get(0));
            } else {
                context.valuePlaceholders.add("?");
            }

            if (f.isPk()) {
                context.pkFieldNames.add(fieldName);
            } else {
                // UPDATE SET fieldName = excluded.fieldName
                context.updateSets.add(String.format("%s = excluded.%s", fieldName, fieldName));
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

    private List<Table> getTables(DatabaseConnectorInstance connectorInstance, String sql, TableTypeEnum type) {
        List<String> tableNames = connectorInstance.execute(databaseTemplate->databaseTemplate.queryForList(sql, String.class));
        if (!CollectionUtils.isEmpty(tableNames)) {
            return tableNames.stream().map(name-> {
                Table table = new Table();
                table.setName(name);
                table.setType(type.getCode());
                return table;
            }).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

}
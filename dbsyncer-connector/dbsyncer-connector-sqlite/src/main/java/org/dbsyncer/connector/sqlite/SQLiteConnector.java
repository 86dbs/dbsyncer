/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.sqlite.validator.SQLiteConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.ArrayList;
import java.util.List;
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

    private final SQLiteConfigValidator configValidator = new SQLiteConfigValidator();

    @Override
    public String getConnectorType() {
        return "SQLite";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance) {
        DatabaseConfig config = connectorInstance.getConfig();
        List<Table> tables = getTables(connectorInstance, String.format(QUERY_TABLE, config.getSchema()), TableTypeEnum.TABLE);
        tables.addAll(getTables(connectorInstance, QUERY_VIEW, TableTypeEnum.VIEW));
        return tables;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        return null;
    }

    @Override
    public String getPageSql(PageSql config) {
        // select * from "my_user" where "id" > ? and "uid" > ? order by "id","uid" limit ? OFFSET ?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        if (PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            appendOrderByPk(config, sql);
        }
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
    public String buildTableName(String tableName) {
        return convertKey(tableName);
    }

    @Override
    public String buildFieldName(Field field) {
        return convertKey(field.getName());
    }

    @Override
    public List<String> buildPrimaryKeys(List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return primaryKeys;
        }
        return primaryKeys.stream().map(pk -> convertKey(pk)).collect(Collectors.toList());
    }

    private List<Table> getTables(DatabaseConnectorInstance connectorInstance, String sql, TableTypeEnum type) {
        List<String> tableNames = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(sql, String.class));
        if (!CollectionUtils.isEmpty(tableNames)) {
            return tableNames.stream().map(name -> new Table(name, type.getCode())).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    private String convertKey(String key) {
        return new StringBuilder("\"").append(key).append("\"").toString();
    }

}
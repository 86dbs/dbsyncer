/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.cdc.MySQLListener;
import org.dbsyncer.connector.mysql.schema.MySQLDateValueMapper;
import org.dbsyncer.connector.mysql.schema.MySQLSchemaResolver;
import org.dbsyncer.connector.mysql.storage.MySQLStorageService;
import org.dbsyncer.connector.mysql.validator.MySQLConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Types;
import java.util.List;

/**
 * MySQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public final class MySQLConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final MySQLConfigValidator configValidator = new MySQLConfigValidator();
    private final MySQLSchemaResolver schemaResolver = new MySQLSchemaResolver();

    public MySQLConnector() {
        VALUE_MAPPERS.put(Types.DATE, new MySQLDateValueMapper());
    }

    @Override
    public String getConnectorType() {
        return "MySQL";
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
            return new MySQLListener();
        }
        return null;
    }

    @Override
    public StorageService getStorageService() {
        return new MySQLStorageService();
    }

    @Override
    public String generateUniqueCode() {
        return DatabaseConstant.DBS_UNIQUE_CODE;
    }

    @Override
    public String buildSqlWithQuotation() {
        return "`";
    }

    @Override
    public String getPageSql(PageSql config) {
        // select * from test.`my_user` where `id` > ? and `uid` > ? order by `id`,`uid` limit ?,?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        if (PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            appendOrderByPk(config, sql);
        }
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            logger.debug("不支持游标查询，主键包含非数字类型");
            return StringUtil.EMPTY;
        }

        // select * from test.`my_user` where `id` > ? and `uid` > ? order by `id`,`uid` limit ?,?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        boolean skipFirst = false;
        // 没有过滤条件
        if (StringUtil.isBlank(config.getQueryFilter())) {
            skipFirst = true;
            sql.append(" WHERE ");
        }
        final String quotation = buildSqlWithQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " > ? ", skipFirst);
        appendOrderByPk(config, sql);
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors) {
            return new Object[]{0, pageSize};
        }
        int cursorsLen = cursors.length;
        Object[] newCursors = new Object[cursorsLen + 2];
        System.arraycopy(cursors, 0, newCursors, 0, cursorsLen);
        newCursors[cursorsLen] = 0;
        newCursors[cursorsLen + 1] = pageSize;
        return newCursors;
    }

    @Override
    public boolean enableCursor() {
        return true;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    protected String getSchema(DatabaseConfig config, Connection connection) {
        return null;
    }
}
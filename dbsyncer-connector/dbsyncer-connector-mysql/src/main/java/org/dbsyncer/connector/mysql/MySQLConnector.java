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
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
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
    protected String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();
        // 新版本使用别名引用新值
        String asNew = getVersion(connectorInstance) >= 8019000000L ? " AS new" : "";
        List<String> fs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        List<String> dfs = new ArrayList<>();
        fields.forEach(f -> {
            String name = database.buildFieldName(f);
            fs.add(name);
            vs.add("?");
            dfs.add(String.format("%s = VALUES(?)", name));
        });

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = buildTableName(config);
        String fieldNames = StringUtil.join(fs, ",");
        String values = StringUtil.join(vs, ",");
        String dupNames = StringUtil.join(dfs, ",");
        // 基于主键或唯一索引冲突时更新
        return String.format("%sINSERT INTO %s (%s) VALUES (%s)%s ON DUPLICATE KEY UPDATE %s",
                uniqueCode, table, fieldNames, values, asNew, dupNames);
    }

    @Override
    protected String buildInsertSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();

        List<String> fs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        fields.forEach(f -> {
            fs.add(database.buildFieldName(f));
            vs.add("?");
        });

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = buildTableName(config);
        String fieldNames = StringUtil.join(fs, ",");
        String values = StringUtil.join(vs, ",");

        // 冲突时忽略插入，不进行任何操作
        return String.format("%sINSERT IGNORE INTO %s (%s) VALUES (%s)", uniqueCode, table, fieldNames, values);
    }

    private StringBuilder buildTableName(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String quotation = database.buildSqlWithQuotation();
        StringBuilder table = new StringBuilder();
        table.append(config.getSchema());
        table.append(quotation);
        table.append(database.buildTableName(config.getTableName()));
        table.append(quotation);
        return table;
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

    /**
     * TODO 性能优化
     *
     * @param connectorInstance
     * @return
     */
    private Long getVersion(DatabaseConnectorInstance connectorInstance) {
        String version = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForObject("SELECT VERSION()", String.class));
        version = version.replace(StringUtil.POINT, StringUtil.EMPTY);
        if (version.contains(StringUtil.HORIZONTAL)) {
            version = version.split(StringUtil.HORIZONTAL)[0];
        }
        return Long.parseLong(String.format("%-10s", version).replace(StringUtil.SPACE, "0"));
    }
}
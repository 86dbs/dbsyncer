/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.postgresql.cdc.PostgreSQLListener;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLBitValueMapper;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLOtherValueMapper;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLSchemaResolver;
import org.dbsyncer.connector.postgresql.validator.PostgreSQLConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    /**
     * PostgreSQL引号字符
     */
    private static final String QUOTATION = "\"";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final PostgreSQLConfigValidator configValidator = new PostgreSQLConfigValidator();

    private final PostgreSQLSchemaResolver schemaResolver = new PostgreSQLSchemaResolver();

    public PostgreSQLConnector() {
        VALUE_MAPPERS.put(Types.BIT, new PostgreSQLBitValueMapper());
        VALUE_MAPPERS.put(Types.OTHER, new PostgreSQLOtherValueMapper());
    }

    @Override
    public String getConnectorType() {
        return "PostgreSQL";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public Map<String, String> getPosition(org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance connectorInstance) {
        // 查询当前WAL位置
        String currentLSN = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject("SELECT pg_current_wal_lsn()", String.class));

        if (currentLSN != null) {
            // 创建与snapshot中存储格式一致的position信息
            Map<String, String> position = new HashMap<>();
            position.put("position", currentLSN);
            return position;
        }
        // 如果无法获取位置信息，返回空Map
        return new HashMap<>();
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
    public String getQuotation() {
        return QUOTATION;
    }

    /**
     * 获取带引号的架构名
     */
    private String getSchemaWithQuotation(DatabaseConfig config) {
        StringBuilder schema = new StringBuilder();
        if (StringUtil.isNotBlank(config.getSchema())) {
            schema.append(QUOTATION).append(config.getSchema()).append(QUOTATION).append(".");
        }
        return schema.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageIndex = context.getPageIndex();
        int pageSize = context.getPageSize();
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors) {
            return new Object[]{pageSize, 0};
        }
        int cursorsLen = cursors.length;
        Object[] newCursors = new Object[cursorsLen + 2];
        System.arraycopy(cursors, 0, newCursors, 0, cursorsLen);
        newCursors[cursorsLen] = pageSize;
        newCursors[cursorsLen + 1] = 0;
        return newCursors;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    protected Map<String, String> buildSourceCommands(CommandConfig commandConfig) {
        Map<String, String> map = new HashMap<>();

        // 获取基础信息
        Table table = commandConfig.getTable();
        String tableName = table.getName();
        String schema = getSchemaWithQuotation((DatabaseConfig) commandConfig.getConnectorConfig());
        List<Field> column = table.getColumn();
        final String queryFilterSql = getQueryFilterSql(commandConfig);

        // 获取缓存的字段列表和基础信息
        String fieldListSql = commandConfig.getCachedFieldListSql();
        String quotedTableName = QUOTATION + buildTableName(tableName) + QUOTATION;
        String cursorCondition = buildCursorConditionFromCached(commandConfig.getCachedPrimaryKeys());

        // 流式查询SQL（直接使用基础查询，PostgreSQL通过fetchSize控制）
        String filterClause = StringUtil.isNotBlank(queryFilterSql) ? " WHERE " + queryFilterSql : "";
        String streamingSql = String.format("SELECT %s FROM %s%s%s",
                fieldListSql, schema, quotedTableName, filterClause);
        map.put(ConnectorConstant.OPERTION_QUERY_STREAM, streamingSql);

        // 游标查询SQL
        if (PrimaryKeyUtil.isSupportedCursor(column)) {
            // 构建完整的WHERE条件：原有过滤条件 + 游标条件
            String whereCondition = "";
            if (StringUtil.isNotBlank(queryFilterSql) && StringUtil.isNotBlank(cursorCondition)) {
                whereCondition = " WHERE " + queryFilterSql + " AND " + cursorCondition;
            } else if (StringUtil.isNotBlank(queryFilterSql)) {
                whereCondition = " WHERE " + queryFilterSql;
            } else if (StringUtil.isNotBlank(cursorCondition)) {
                whereCondition = " WHERE " + cursorCondition;
            }

            String cursorSql = String.format("SELECT %s FROM %s%s%s ORDER BY %s LIMIT ? OFFSET ?",
                    fieldListSql, schema, quotedTableName, whereCondition, commandConfig.getCachedPrimaryKeys());
            map.put(ConnectorConstant.OPERTION_QUERY_CURSOR, cursorSql);
        }

        // 计数SQL
        String countSql = String.format("SELECT COUNT(1) FROM %s%s%s",
                schema, quotedTableName, filterClause);
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, countSql);

        return map;
    }


    /**
     * 基于缓存的主键列表构建游标条件内容（不包含WHERE关键字）
     */
    private String buildCursorConditionFromCached(String cachedPrimaryKeys) {
        if (StringUtil.isBlank(cachedPrimaryKeys)) {
            return "";
        }

        // 将 ""id", "name", "create_time"" 转换为 ""id" > ? AND "name" > ? AND "create_time" > ?"
        return cachedPrimaryKeys.replaceAll(",", " > ? AND") + " > ?";
    }


}
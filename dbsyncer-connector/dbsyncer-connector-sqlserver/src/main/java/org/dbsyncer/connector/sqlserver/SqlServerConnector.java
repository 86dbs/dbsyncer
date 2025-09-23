/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.sqlserver.cdc.Lsn;
import org.dbsyncer.connector.sqlserver.cdc.SqlServerListener;
import org.dbsyncer.connector.sqlserver.validator.SqlServerConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SqlServer连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class SqlServerConnector extends AbstractDatabaseConnector {

    /**
     * SQL Server引号字符
     */
    private static final String QUOTATION = "[";

    private final String QUERY_VIEW = "select name from sysobjects where xtype in('v')";
    private final String QUERY_TABLE = "select name from sys.tables where schema_id = schema_id('%s') and is_ms_shipped = 0";
    private final String QUERY_TABLE_IDENTITY = "select is_identity from sys.columns where object_id = object_id('%s') and is_identity > 0";
    private final String SET_TABLE_IDENTITY_ON = "set identity_insert %s.[%s] on;";
    private final String SET_TABLE_IDENTITY_OFF = ";set identity_insert %s.[%s] off;";

    private final SqlServerConfigValidator configValidator = new SqlServerConfigValidator();

    /**
     * 获取带引号的架构名
     */
    private String getSchemaWithQuotation(DatabaseConfig config) {
        StringBuilder schema = new StringBuilder();
        if (StringUtil.isNotBlank(config.getSchema())) {
            schema.append(QUOTATION).append(config.getSchema()).append("].");
        }
        return schema.toString();
    }

    @Override
    public String getConnectorType() {
        return "SqlServer";
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

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new SqlServerListener();
        }
        return null;
    }


    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
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

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Map<String, String> targetCommand = super.getTargetCommand(commandConfig);
        String tableName = commandConfig.getTable().getName();
        // 判断表是否包含标识自增列
        DatabaseConnectorInstance db = (DatabaseConnectorInstance) commandConfig.getConnectorInstance();
        List<Integer> result = db.execute(databaseTemplate -> databaseTemplate.queryForList(String.format(QUERY_TABLE_IDENTITY, tableName), Integer.class));
        // 允许显式插入标识列的值
        if (!CollectionUtils.isEmpty(result)) {
            DatabaseConfig config = (DatabaseConfig) commandConfig.getConnectorConfig();
            String insert = String.format(SET_TABLE_IDENTITY_ON, config.getSchema(), tableName)
                    + targetCommand.get(ConnectorConstant.OPERTION_INSERT)
                    + String.format(SET_TABLE_IDENTITY_OFF, config.getSchema(), tableName);
            targetCommand.put(ConnectorConstant.OPERTION_INSERT, insert);
        }
        return targetCommand;
    }

    @Override
    public Map<String, String> getPosition(DatabaseConnectorInstance connectorInstance) {
        // 查询当前LSN位置
        byte[] currentLsnBytes = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject("SELECT sys.fn_cdc_get_max_lsn()", byte[].class));

        if (currentLsnBytes == null) {
            throw new RuntimeException("获取SqlServer当前LSN失败");
        }

        // 将 byte[] 转换为 LSN 字符串表示
        String currentLsn = new Lsn(currentLsnBytes).toString();

        // 创建与snapshot中存储格式一致的position信息
        Map<String, String> position = new HashMap<>();
        position.put("position", currentLsn);
        return position;
    }

    private String convertKey(String key) {
        return new StringBuilder("[").append(key).append("]").toString();
    }

    @Override
    public boolean enableCursor() {
        return true;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }


    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors) {
            return new Object[]{pageSize}; // 只有 TOP 参数
        }
        int cursorsLen = cursors.length;
        Object[] newCursors = new Object[cursorsLen + 1];
        newCursors[0] = pageSize; // TOP 参数在前
        System.arraycopy(cursors, 0, newCursors, 1, cursorsLen); // 游标参数在后
        return newCursors;
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
        String quotedTableName = QUOTATION + buildTableName(tableName) + "]";
        String cursorCondition = buildCursorConditionFromCached(commandConfig.getCachedPrimaryKeys());
        String filterClause = StringUtil.isNotBlank(queryFilterSql) ? " WHERE " + queryFilterSql : "";

        // 流式查询SQL（直接使用基础查询，SQL Server通过fetchSize控制）
        String streamingSql = String.format("SELECT %s FROM %s%s%s",
                fieldListSql, schema, quotedTableName, filterClause);
        map.put(ConnectorConstant.OPERTION_QUERY_STREAM, streamingSql);

        // 游标查询SQL
        if (enableCursor() && PrimaryKeyUtil.isSupportedCursor(column)) {
            // 构建完整的WHERE条件：原有过滤条件 + 游标条件
            String whereCondition = "";
            if (StringUtil.isNotBlank(filterClause) && StringUtil.isNotBlank(cursorCondition)) {
                whereCondition = filterClause + " AND " + cursorCondition;
            } else if (StringUtil.isNotBlank(filterClause)) {
                whereCondition = filterClause;
            } else if (StringUtil.isNotBlank(cursorCondition)) {
                whereCondition = " WHERE " + cursorCondition;
            }

            String cursorSql = String.format("SELECT %s FROM %s%s%s ORDER BY %s OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
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

        // 将 "[id], [name], [create_time]" 转换为 "[id] > ? AND [name] > ? AND [create_time] > ?"
        return cachedPrimaryKeys.replaceAll(",", " > ? AND") + " > ?";
    }


}
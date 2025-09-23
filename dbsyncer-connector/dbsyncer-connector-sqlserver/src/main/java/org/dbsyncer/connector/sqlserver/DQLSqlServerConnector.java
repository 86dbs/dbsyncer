/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.connector.sqlserver.cdc.DqlSqlServerListener;
import org.dbsyncer.connector.sqlserver.validator.DqlSqlServerConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.dbsyncer.common.util.StringUtil;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * DQLSqlServer连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class DQLSqlServerConnector extends AbstractDQLConnector {

    private final DqlSqlServerConfigValidator configValidator = new DqlSqlServerConfigValidator();

    @Override
    public String getConnectorType() {
        return "DqlSqlServer";
    }

    @Override
    public ConfigValidator<?> getConfigValidator() {
        return configValidator;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new DqlSqlServerListener();
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
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }

    @Override
    protected Map<String, String> buildSourceCommands(CommandConfig commandConfig) {
        Map<String, String> map = new HashMap<>();
        
        // 获取基础信息
        Table table = commandConfig.getTable();
        String tableName = table.getName();
        List<Field> column = table.getColumn();
        final String queryFilterSql = getQueryFilterSql(commandConfig);
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);
        
        // 构建基础查询SQL
        String baseQuerySql = buildBaseQuerySql(tableName, column, queryFilterSql);
        
        // 流式查询SQL（直接使用基础查询，SQL Server通过fetchSize控制）
        map.put(ConnectorConstant.OPERTION_QUERY_STREAM, baseQuerySql);
        
        // 计数SQL
        String countSql = buildCountSql(tableName, queryFilterSql);
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, countSql);
        
        return map;
    }

    /**
     * 构建基础查询SQL
     */
    private String buildBaseQuerySql(String tableName, List<Field> column, String queryFilterSql) {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // 构建字段列表
        int size = column.size();
        int end = size - 1;
        for (int i = 0; i < size; i++) {
            Field field = column.get(i);
            sql.append(field.getName());
            if (i < end) {
                sql.append(", ");
            }
        }
        
        sql.append(" FROM ").append(tableName);
        if (StringUtil.isNotBlank(queryFilterSql)) {
            sql.append(" ").append(queryFilterSql);
        }
        
        return sql.toString();
    }

    /**
     * 构建分页查询SQL
     */
    private String buildPaginationQuerySql(String baseQuerySql, List<String> primaryKeys) {
        String orderBy = StringUtil.join(primaryKeys, StringUtil.COMMA);
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, orderBy, baseQuerySql);
    }

    /**
     * 构建计数SQL
     */
    private String buildCountSql(String tableName, String queryFilterSql) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM ").append(tableName);
        if (StringUtil.isNotBlank(queryFilterSql)) {
            sql.append(" ").append(queryFilterSql);
        }
        return sql.toString();
    }
}
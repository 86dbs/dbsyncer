/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle;

import org.dbsyncer.connector.oracle.cdc.DqlOracleListener;
import org.dbsyncer.connector.oracle.validator.DqlOracleConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.dbsyncer.common.util.StringUtil;
import org.springframework.util.CollectionUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DQLOracle连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public final class DQLOracleConnector extends AbstractDQLConnector {

    /**
     * Oracle引号字符
     */
    private static final String QUOTATION = "\"";

    private final DqlOracleConfigValidator configValidator = new DqlOracleConfigValidator();

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
    public String getConnectorType() {
        return "DqlOracle";
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
            return new DqlOracleListener();
        }
        return null;
    }


    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{pageIndex * pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public String getQuotation() {
        return QUOTATION;
    }

    @Override
    public String getValidationQuery() {
        return "select 1 from dual";
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
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);

        // 获取缓存的字段列表和基础信息
        String fieldListSql = commandConfig.getCachedFieldListSql();
        String quotedTableName = QUOTATION + buildTableName(tableName) + QUOTATION;
        String orderByClause = buildOrderByClause(primaryKeys);
        String cursorCondition = buildCursorCondition(primaryKeys, queryFilterSql);

        // 流式查询SQL（直接使用基础查询，Oracle通过fetchSize控制）
        String streamingSql = String.format("SELECT %s FROM %s%s%s",
            fieldListSql, schema, quotedTableName, queryFilterSql);
        map.put(ConnectorConstant.OPERTION_QUERY_STREAM, streamingSql);

        // 游标查询SQL
        if (enableCursor() && PrimaryKeyUtil.isSupportedCursor(column)) {
            String cursorSql = String.format("SELECT * FROM (SELECT A.*, ROWNUM RN FROM (SELECT %s FROM %s%s%s%s%s)A WHERE ROWNUM <= ?) WHERE RN > ?",
                fieldListSql, schema, quotedTableName, queryFilterSql, cursorCondition, orderByClause);
            map.put(ConnectorConstant.OPERTION_QUERY_CURSOR, cursorSql);
        }

        // 计数SQL
        String countSql = String.format("SELECT COUNT(1) FROM %s%s%s",
            schema, quotedTableName, queryFilterSql);
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, countSql);

        return map;
    }

    /**
     * 构建ORDER BY子句
     */
    private String buildOrderByClause(List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return "";
        }
        
        StringBuilder orderBy = new StringBuilder(" ORDER BY ");
        
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                orderBy.append(", ");
            }
            Field field = new Field();
            field.setName(primaryKeys.get(i));
            orderBy.append(QUOTATION).append(buildFieldName(field)).append(QUOTATION);
        }
        
        return orderBy.toString();
    }

    /**
     * 构建游标条件
     */
    private String buildCursorCondition(List<String> primaryKeys, String queryFilterSql) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return "";
        }
        
        StringBuilder condition = new StringBuilder();
        
        // 如果没有过滤条件，需要添加WHERE
        if (StringUtil.isBlank(queryFilterSql)) {
            condition.append(" WHERE ");
        } else {
            condition.append(" AND ");
        }
        
        // 构建游标条件
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                condition.append(" AND ");
            }
            Field field = new Field();
            field.setName(primaryKeys.get(i));
            condition.append(QUOTATION).append(buildFieldName(field)).append(QUOTATION).append(" > ?");
        }
        
        return condition.toString();
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }

}
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
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.impl.MySQLTemplate;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplateType;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public final class MySQLConnector extends AbstractDatabaseConnector {

    /**
     * MySQL引号字符
     */
    private static final String QUOTATION = "`";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final MySQLConfigValidator configValidator = new MySQLConfigValidator();
    private final MySQLSchemaResolver schemaResolver = new MySQLSchemaResolver();
    private final SqlTemplate sqlTemplate = new MySQLTemplate();

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
    public Map<String, String> getPosition(
            org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance connectorInstance) {
        // 执行SHOW MASTER STATUS命令获取当前binlog位置
        Map<String, Object> result = connectorInstance
                .execute(databaseTemplate -> databaseTemplate.queryForMap("SHOW MASTER STATUS"));

        if (result == null || result.isEmpty()) {
            throw new RuntimeException("获取MySQL当前binlog位置失败");
        }
        Map<String, String> position = new HashMap<>();
        position.put("fileName", (String) result.get("File"));
        position.put("position", String.valueOf(result.get("Position")));
        return position;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return Integer.MIN_VALUE; // MySQL流式处理特殊值
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    protected Map<String, String> buildSourceCommands(CommandConfig commandConfig) {
        Map<String, String> map = new HashMap<>();

        // 创建构建上下文
        SqlBuildContext buildContext = createBuildContext(commandConfig);
        
        // 构建流式查询SQL
        String streamingSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_STREAM, buildContext);
        map.put(ConnectorConstant.OPERTION_QUERY_STREAM, streamingSql);
        
        // 构建游标查询SQL
        if (PrimaryKeyUtil.isSupportedCursor(commandConfig.getTable().getColumn())) {
            String cursorSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_CURSOR, buildContext);
            map.put(ConnectorConstant.OPERTION_QUERY_CURSOR, cursorSql);
        }
        
        // 构建计数SQL
        String countSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_COUNT, buildContext);
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, countSql);
        
        return map;
    }
    
    private SqlBuildContext createBuildContext(CommandConfig commandConfig) {
        Table table = commandConfig.getTable();
        DatabaseConfig dbConfig = (DatabaseConfig) commandConfig.getConnectorConfig();
        
        SqlBuildContext buildContext = new SqlBuildContext();
        buildContext.setSchema(getSchemaWithQuotation(dbConfig));
        buildContext.setTableName(QUOTATION + buildTableName(table.getName()) + QUOTATION);
        buildContext.setFields(table.getColumn());
        buildContext.setPrimaryKeys(PrimaryKeyUtil.findTablePrimaryKeys(table));
        buildContext.setQueryFilter(getQueryFilterSql(commandConfig));
        buildContext.setCursorCondition(buildCursorConditionFromCached(commandConfig.getCachedPrimaryKeys()));
        
        return buildContext;
    }

    /**
     * 基于缓存的主键列表构建游标条件内容（不包含WHERE关键字）
     */
    private String buildCursorConditionFromCached(String cachedPrimaryKeys) {
        if (StringUtil.isBlank(cachedPrimaryKeys)) {
            return "";
        }

        // 将 "`id`, `name`, `create_time`" 转换为 "`id` > ? AND `name` > ? AND
        // `create_time` > ?"
        return cachedPrimaryKeys.replaceAll(",", " > ? AND") + " > ?";
    }

}
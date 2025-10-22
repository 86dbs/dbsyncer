/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.sqlserver.bulk.SqlServerBulkCopyUtil;
import org.dbsyncer.connector.sqlserver.cdc.Lsn;
import org.dbsyncer.connector.sqlserver.cdc.SqlServerListener;
import org.dbsyncer.connector.sqlserver.schema.SqlServerSchemaResolver;
import org.dbsyncer.connector.sqlserver.validator.SqlServerConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.sql.Connection;
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
public class SqlServerConnector extends AbstractDatabaseConnector {


    private final String QUERY_VIEW = "select name from sysobjects where xtype in('v')";
    private final String QUERY_TABLE = "select name from sys.tables where schema_id = schema_id('%s') and is_ms_shipped = 0";
    private final String QUERY_TABLE_IDENTITY = "select is_identity from sys.columns where object_id = object_id('%s') and is_identity > 0";
    private final String SET_TABLE_IDENTITY_ON = "set identity_insert %s.[%s] on;";
    private final String SET_TABLE_IDENTITY_OFF = ";set identity_insert %s.[%s] off;";

    private final SqlServerSchemaResolver schemaResolver = new SqlServerSchemaResolver();

    public SqlServerConnector() {
        sqlTemplate = new SqlServerTemplate();
        configValidator = new SqlServerConfigValidator();
    }

    @Override
    public String getConnectorType() {
        return "SqlServer";
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

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    /**
     * 重写 writer 方法，对 SQL Server 使用批量复制优化
     */
    @Override
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        String event = context.getEvent();
        List<Map> data = context.getTargetList();
        List<Field> targetFields = context.getTargetFields();

        // 只对 INSERT 操作且数据量大于阈值时使用批量复制
        if (ConnectorConstant.OPERTION_INSERT.equals(event) &&
                !CollectionUtils.isEmpty(data)) {
            try {
                return connectorInstance.execute(databaseTemplate -> {
                    SimpleConnection connection = databaseTemplate.getSimpleConnection();
                    // 获取 schema 名称
                    String schemaName = connectorInstance.getConfig().getSchema();
                    if (schemaName == null || schemaName.trim().isEmpty()) {
                        schemaName = "dbo"; // 默认 schema
                    }

                    // 如果强制更新，使用 UPSERT；否则使用普通插入
                    if (context.isForceUpdate()) {
                        return bulkUpsert(connection, context, targetFields, data, schemaName);
                    } else {
                        return bulkInsert(connection, context, targetFields, data, schemaName);
                    }
                });
            } catch (Exception e) {
                Result result = new Result();
                result.error = e.getMessage();
                result.addFailData(context.getTargetList());
                if (context.isEnablePrintTraceInfo()) {
                    logger.error("traceId:{}, tableName:{}, event:{}, targetList:{}, result:{}", context.getTraceId(), context.getSourceTableName(),
                            context.getEvent(), context.getTargetList(), JsonUtil.objToJson(result));
                }
                return result;
            }
        }

        // 其他情况使用标准插入
        return super.writer(connectorInstance, context);
    }

    /**
     * 执行批量插入
     */
    private Result bulkInsert(Connection connection, PluginContext context,
                              List<Field> targetFields, List<Map> data, String schemaName) throws Exception {
        Result result = new Result();

        // 获取表名
        String tableName = context.getTargetTableName();

        // 执行批量插入
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> typedData = new java.util.ArrayList<>();
        for (Map map : data) {
            typedData.add((Map<String, Object>) map);
        }

        int insertedCount = SqlServerBulkCopyUtil.bulkInsert(connection, tableName, targetFields, typedData, schemaName);

        // 设置成功数据
        result.addSuccessData(data);

        logger.info("SQL Server 批量插入完成，表: {}, 插入记录数: {}", tableName, insertedCount);

        return result;
    }

    /**
     * 执行批量 UPSERT
     */
    private Result bulkUpsert(Connection connection, PluginContext context,
                              List<Field> targetFields, List<Map> data, String schemaName) throws Exception {
        Result result = new Result();

        // 获取表名
        String tableName = context.getTargetTableName();
        // 获取主键字段
        List<String> primaryKeys = targetFields.stream()
                .filter(Field::isPk)
                .map(Field::getName)
                .collect(java.util.stream.Collectors.toList());

        if (primaryKeys.isEmpty()) {
            logger.warn("表 {} 没有主键，无法执行 UPSERT，回退到普通插入", tableName);
            return bulkInsert(connection, context, targetFields, data, schemaName);
        }

        // 执行批量 UPSERT
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> typedData = new java.util.ArrayList<>();
        for (Map map : data) {
            typedData.add((Map<String, Object>) map);
        }

        int processedCount = SqlServerBulkCopyUtil.bulkUpsert(connection, tableName, targetFields, typedData, primaryKeys, schemaName);

        // 设置成功数据
        result.addSuccessData(data);

        logger.info("SQL Server 批量 UPSERT 完成，表: {}, 处理记录数: {}", tableName, processedCount);

        return result;
    }
}
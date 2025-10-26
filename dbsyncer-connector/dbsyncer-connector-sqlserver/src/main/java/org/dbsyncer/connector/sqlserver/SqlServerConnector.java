/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
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
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
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
    private final String QUERY_TABLE_IDENTITY = "select is_identity from sys.columns where object_id = object_id('%s') and is_identity > 0";
    private final String SET_TABLE_IDENTITY_ON = "set identity_insert %s.[%s] on;";
    private final String SET_TABLE_IDENTITY_OFF = ";set identity_insert %s.[%s] off;";

    private final SqlServerConfigValidator configValidator = new SqlServerConfigValidator();

    @Override
    public String getConnectorType() {
        return "SqlServer";
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
            return new SqlServerListener();
        }
        return null;
    }

    @Override
    public String getPageSql(PageSql config) {
        List<String> primaryKeys = buildPrimaryKeys(config.getPrimaryKeys());
        String orderBy = StringUtil.join(primaryKeys, StringUtil.COMMA);
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, orderBy, config.getQuerySql());
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
        return primaryKeys.stream().map(this::convertKey).collect(Collectors.toList());
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
            String insert = String.format(SET_TABLE_IDENTITY_ON, commandConfig.getSchema(), tableName)
                    + targetCommand.get(ConnectorConstant.OPERTION_INSERT)
                    + String.format(SET_TABLE_IDENTITY_OFF, commandConfig.getSchema(), tableName);
            targetCommand.put(ConnectorConstant.OPERTION_INSERT, insert);
        }
        return targetCommand;
    }

    @Override
    public Object getPosition(DatabaseConnectorInstance connectorInstance) {
        String sql = "SELECT * from cdc.lsn_time_mapping order by tran_begin_time desc";
        List<Map<String, Object>> result = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(sql));
        if (!CollectionUtils.isEmpty(result)) {
            List<Object> list = new ArrayList<>();
            result.forEach(r -> {
                r.computeIfPresent("start_lsn", (k, lsn)-> new Lsn((byte[]) lsn).toString());
                r.computeIfPresent("tran_begin_lsn", (k, lsn)-> new Lsn((byte[]) lsn).toString());
                r.computeIfPresent("tran_id", (k, lsn)-> new Lsn((byte[]) lsn).toString());
                r.computeIfPresent("tran_begin_time", (k, tranBeginTime)-> DateFormatUtil.timestampToString((Timestamp) tranBeginTime));
                r.computeIfPresent("tran_end_time", (k, tranEndTime)-> DateFormatUtil.timestampToString((Timestamp) tranEndTime));
                list.add(r);
            });
            return list;
        }
        return result;
    }

    @Override
    protected String getSchema(String schema, Connection connection) {
        return StringUtil.isNotBlank(schema) ? schema : "dbo";
    }

    private String convertKey(String key) {
        return "[" + key + "]";
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:sqlserver://127.0.0.1:1433;databaseName=test;encrypt=false;trustServerCertificate=true
        StringBuilder url = new StringBuilder();
        url.append("jdbc:sqlserver://").append(config.getHost()).append(":").append(config.getPort());
        if (StringUtil.isNotBlank(database)) {
            url.append(";databaseName=").append(database);
        }
        return url.toString();
    }
}
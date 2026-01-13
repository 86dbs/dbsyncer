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
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.ReaderContext;

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

    private final String QUERY_VIEW = "select name from sysobjects where xtype in('v')";
    private final String QUERY_TABLE = "select name from sys.tables where schema_id = schema_id('%s') and is_ms_shipped = 0";
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
    protected String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();
        
        List<String> fs = new ArrayList<>();
        List<String> sfs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        List<String> updateSets = new ArrayList<>();
        List<String> pkFieldNames = new ArrayList<>();
        
        fields.forEach(f -> {
            String fieldName = database.buildFieldName(f);
            fs.add(fieldName);
            sfs.add("s." + fieldName);
            vs.add("?");
            if (f.isPk()) {
                pkFieldNames.add(fieldName);
            } else {
                updateSets.add(String.format("%s = s.%s", fieldName, fieldName));
            }
        });
        
        StringBuilder sql = new StringBuilder(database.generateUniqueCode());
        sql.append("MERGE ").append(config.getSchema());
        sql.append(database.buildTableName(config.getTableName())).append(" AS t ");
        sql.append("USING (VALUES (").append(StringUtil.join(vs, StringUtil.COMMA)).append(")) AS s (");
        sql.append(StringUtil.join(fs, StringUtil.COMMA)).append(") ");
        sql.append("ON ");
        
        // 构建 ON 条件：t.pk = s.pk AND ...
        StringBuilder onCondition = new StringBuilder();
        for (int i = 0; i < pkFieldNames.size(); i++) {
            if (i > 0) {
                onCondition.append(" AND ");
            }
            String pkFieldName = pkFieldNames.get(i);
            onCondition.append("t.").append(pkFieldName).append(" = s.").append(pkFieldName);
        }
        sql.append(onCondition).append(" ");
        
        sql.append("WHEN MATCHED THEN UPDATE SET ");
        sql.append(StringUtil.join(updateSets, StringUtil.COMMA)).append(" ");
        sql.append("WHEN NOT MATCHED THEN INSERT (");
        sql.append(StringUtil.join(fs, StringUtil.COMMA)).append(") VALUES (");
        sql.append(StringUtil.join(sfs, StringUtil.COMMA)).append(");");
        
        return sql.toString();
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
        String table = config.getSchema() + database.buildTableName(config.getTableName());
        String fieldNames = StringUtil.join(fs, StringUtil.COMMA);
        String values = StringUtil.join(vs, StringUtil.COMMA);
        
        return String.format("%sINSERT INTO %s (%s) VALUES (%s)", uniqueCode, table, fieldNames, values);
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

    private String convertKey(String key) {
        return new StringBuilder("[").append(key).append("]").toString();
    }

}
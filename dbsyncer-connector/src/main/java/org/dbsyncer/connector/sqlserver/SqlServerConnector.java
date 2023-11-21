package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.quartz.DatabaseQuartzListener;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public final class SqlServerConnector extends AbstractDatabaseConnector {

    private final String QUERY_VIEW = "select name from sysobjects where xtype in('v')";

    private final String QUERY_TABLE = "select name from sys.tables where schema_id = schema_id('%s') and is_ms_shipped = 0";

    private final String TYPE = "SqlServer";

    @Override
    public String getConnectorType() {
        return TYPE;
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorMapper) {
        DatabaseConfig config = connectorMapper.getConfig();
        List<Table> tables = getTables(connectorMapper, String.format(QUERY_TABLE, config.getSchema()), TableTypeEnum.TABLE);
        tables.addAll(getTables(connectorMapper, QUERY_VIEW, TableTypeEnum.VIEW));
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
        String orderBy = StringUtil.join(primaryKeys, ",");
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, orderBy, config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
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

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, List<String> primaryKeys, String schema, String queryFilterSql) {
        // 视图或有过滤条件，走默认方式
        final Table table = commandConfig.getTable();
        if (StringUtil.isNotBlank(queryFilterSql) || TableTypeEnum.isView(table.getType())) {
            return super.getQueryCountSql(commandConfig, primaryKeys, schema, queryFilterSql);
        }

        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        // 从存储过程查询（定时更新总数，可能存在误差）
        return String.format("select rows from sysindexes where id = object_id('%s.%s') and indid in (0, 1)", cfg.getSchema(),
                buildTableName(table.getName()));
    }

    private List<Table> getTables(DatabaseConnectorInstance connectorMapper, String sql, TableTypeEnum type) {
        List<String> tableNames = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(sql, String.class));
        if (!CollectionUtils.isEmpty(tableNames)) {
            return tableNames.stream().map(name -> new Table(name, type.getCode())).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    private String convertKey(String key) {
        return new StringBuilder("[").append(key).append("]").toString();
    }

}
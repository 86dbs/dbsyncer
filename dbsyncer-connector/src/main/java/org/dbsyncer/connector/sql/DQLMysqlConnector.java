package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;

import java.util.List;
import java.util.Map;

public final class DQLMysqlConnector extends AbstractDatabaseConnector {

    @Override
    protected String getTablesSql(DatabaseConfig config) {
        return "show tables";
    }

    @Override
    public String getPageSql(PageSqlBuilderConfig config) {
        return config.getQuerySql() + DatabaseConstant.MYSQL_PAGE_SQL;
    }

    @Override
    public PageArgConfig prepareSetArgs(String sql, int pageIndex, int pageSize) {
        return new PageArgConfig(sql, new Object[] {(pageIndex - 1) * pageSize, pageSize});
    }

    @Override
    public List<String> getTable(ConnectorConfig config) {
        return super.getDqlTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(ConnectorConfig config, String tableName) {
        return super.getDqlMetaInfo(config);
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return super.getDqlSourceCommand(commandConfig, true);
    }
}
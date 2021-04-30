package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.MetaInfo;
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
    public String getTableColumnSql(String querySQL) {
        // Mysql 表列查询
        return querySQL + DatabaseConstant.MYSQL_TABLE_COLUMN_SQL;
    }

    @Override
    public String getPageSql(String tableName, String pk, String querySQL) {
        // Mysql 分页查询
        return querySQL + DatabaseConstant.MYSQL_PAGE_SQL;
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[] {(pageIndex - 1) * pageSize, pageSize};
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
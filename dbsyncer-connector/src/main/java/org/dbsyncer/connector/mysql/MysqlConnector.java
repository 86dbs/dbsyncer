package org.dbsyncer.connector.mysql;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PageArgConfig;
import org.dbsyncer.connector.config.PageSqlBuilderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;

public final class MysqlConnector extends AbstractDatabaseConnector {

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

}
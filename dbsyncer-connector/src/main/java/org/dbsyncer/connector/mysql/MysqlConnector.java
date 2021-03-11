package org.dbsyncer.connector.mysql;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;

public final class MysqlConnector extends AbstractDatabaseConnector {

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
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

}
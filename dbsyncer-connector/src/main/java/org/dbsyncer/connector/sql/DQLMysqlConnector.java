package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;

public final class DQLMysqlConnector extends AbstractDatabaseConnector {

    @Override
    public String getMetaSql(DatabaseConfig config, String tableName) {
        return config.getSql();
    }

    @Override
    public String getPageSql(DatabaseConfig config, String tableName, String pk, String querySQL) {
        // Mysql 分页查询
        return querySQL + DatabaseConstant.MYSQL_PAGE_SQL;
    }

}
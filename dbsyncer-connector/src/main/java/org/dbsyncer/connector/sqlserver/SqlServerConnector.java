package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;

public final class SqlServerConnector extends AbstractDatabaseConnector implements SqlServer {

    @Override
    public String getMetaSql(DatabaseConfig config, String tableName) {
        return new StringBuilder().append("SELECT * FROM ").append(tableName).toString();
    }

    @Override
    public String getPageSql(DatabaseConfig config, String tableName, String pk, String querySQL) {
        // SqlServer 分页查询
        // sql> SELECT * FROM SD_USER
        // sql> SELECT * FROM SD_USER ORDER BY USER.ID OFFSET(3-1) * 1000 ROWS FETCH NEXT 1000 ROWS ONLY
        return new StringBuilder(querySQL).append(" ORDER BY ").append(tableName).append(".").append(pk).append(" OFFSET(?-1) * ? ROWS FETCH NEXT ? ROWS ONLY").toString();
    }

}
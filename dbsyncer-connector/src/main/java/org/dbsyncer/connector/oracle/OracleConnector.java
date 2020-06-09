package org.dbsyncer.connector.oracle;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;

public final class OracleConnector extends AbstractDatabaseConnector {

    @Override
    public String getMetaSql(DatabaseConfig config, String tableName) {
        return String.format("select * from \"%s\"", tableName);
    }

    @Override
    protected String getQueryTablesSql(DatabaseConfig config) {
        // "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='AE86'"
        return String.format("SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='%s'", config.getUsername()).toUpperCase();
    }

    @Override
    protected String getQueryCountSql(String tableName) {
        return String.format("select count(*) from \"%s\"", tableName);
    }

    @Override
    public String getPageSql(String tableName, String pk, String querySQL) {
        // Oracle 分页查询
        return DatabaseConstant.ORACLE_PAGE_SQL_START + querySQL + DatabaseConstant.ORACLE_PAGE_SQL_END;
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{pageIndex * pageSize, (pageIndex - 1) * pageSize};
    }

}
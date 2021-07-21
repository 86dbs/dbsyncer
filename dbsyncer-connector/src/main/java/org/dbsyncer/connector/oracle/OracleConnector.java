package org.dbsyncer.connector.oracle;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PageSqlConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;

public final class OracleConnector extends AbstractDatabaseConnector {

    @Override
    protected String getTableSql(DatabaseConfig config) {
        return String.format("SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='%s'", config.getUsername()).toUpperCase();
    }

    @Override
    public String getPageSql(PageSqlConfig config) {
        return DatabaseConstant.ORACLE_PAGE_SQL_START + config.getQuerySql() + DatabaseConstant.ORACLE_PAGE_SQL_END;
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[] {pageIndex * pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    protected String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    protected String getValidationQuery() {
        return "select 1 from dual";
    }
}
package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.model.PageSql;

public final class DQLOracleConnector extends AbstractDQLConnector {

    @Override
    public String getPageSql(PageSql config) {
        return DatabaseConstant.ORACLE_PAGE_SQL_START + config.getQuerySql() + DatabaseConstant.ORACLE_PAGE_SQL_END;
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{pageIndex * pageSize, (pageIndex - 1) * pageSize};
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
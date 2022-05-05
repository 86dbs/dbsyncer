package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.constant.DatabaseConstant;

public final class DQLPostgreSQLConnector extends AbstractDQLConnector {

    @Override
    public String getPageSql(PageSql config) {
        return config.getQuerySql() + DatabaseConstant.POSTGRESQL_PAGE_SQL;
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    protected String buildSqlWithQuotation() {
        return "\"";
    }
}
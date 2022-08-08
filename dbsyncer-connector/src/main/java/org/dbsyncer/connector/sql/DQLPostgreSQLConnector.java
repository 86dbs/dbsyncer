package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.model.PageSql;

public final class DQLPostgreSQLConnector extends AbstractDQLConnector {

    @Override
    public String getPageSql(PageSql config) {
        return config.getQuerySql() + DatabaseConstant.POSTGRESQL_PAGE_SQL;
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    protected String buildSqlWithQuotation() {
        return "\"";
    }
}
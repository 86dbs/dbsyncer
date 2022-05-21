package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.model.PageSql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DQLSqlServerConnector extends AbstractDQLConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String getPageSql(PageSql config) {
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, config.getPk(), config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }

}
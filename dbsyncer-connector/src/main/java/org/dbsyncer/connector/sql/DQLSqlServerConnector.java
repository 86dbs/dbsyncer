package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.util.List;

public final class DQLSqlServerConnector extends AbstractDQLConnector {

    @Override
    public String getPageSql(PageSql config) {
        String quotation = config.getQuotation();
        List<String> primaryKeys = config.getPrimaryKeys();
        StringBuilder orderBy = new StringBuilder();
        PrimaryKeyUtil.buildSql(orderBy, primaryKeys, quotation, " AND ", " = ? ", true);
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, orderBy.toString(), config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }

}
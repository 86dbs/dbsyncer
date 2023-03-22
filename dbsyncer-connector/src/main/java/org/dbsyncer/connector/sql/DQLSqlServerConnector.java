package org.dbsyncer.connector.sql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.util.List;

public final class DQLSqlServerConnector extends AbstractDQLConnector {

    @Override
    public String getPageSql(PageSql config) {
        List<String> primaryKeys = config.getPrimaryKeys();
        String orderBy = StringUtil.join(primaryKeys, ",");
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, orderBy, config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }

}
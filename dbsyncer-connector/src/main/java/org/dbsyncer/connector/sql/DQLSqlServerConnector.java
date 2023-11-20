package org.dbsyncer.connector.sql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.model.PageSql;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public final class DQLSqlServerConnector extends AbstractDQLConnector {

    private final String TYPE = "DqlSqlServer";

    @Override
    public String getConnectorType() {
        return TYPE;
    }

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
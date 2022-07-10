package org.dbsyncer.connector.mysql;

import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;

import java.util.List;

public final class MysqlConnector extends AbstractDatabaseConnector {

    @Override
    protected String buildSqlWithQuotation() {
        return "`";
    }

    @Override
    public String getPageSql(PageSql config) {
        return config.getQuerySql() + DatabaseConstant.MYSQL_PAGE_SQL;
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[] {(pageIndex - 1) * pageSize, pageSize};
    }

    @Override
    public List<Table> getTable(DatabaseConnectorMapper connectorMapper) {
        return super.getTable(connectorMapper, "show tables");
    }
}
package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.PageSqlConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;

import java.util.Map;

public final class DQLMysqlConnector extends AbstractDQLConnector {

    @Override
    public String getPageSql(PageSqlConfig config) {
        return config.getQuerySql() + DatabaseConstant.MYSQL_PAGE_SQL;
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return super.getDqlSourceCommand(commandConfig, true);
    }
}
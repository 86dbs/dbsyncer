package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;

import java.util.List;
import java.util.Map;

public final class DQLOracleConnector extends AbstractDatabaseConnector {

    @Override
    protected String getTablesSql(DatabaseConfig config) {
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
    public List<String> getTable(ConnectorMapper config) {
        return super.getDqlTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(ConnectorMapper config, String tableName) {
        return super.getDqlMetaInfo(config);
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return super.getDqlSourceCommand(commandConfig, false);
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
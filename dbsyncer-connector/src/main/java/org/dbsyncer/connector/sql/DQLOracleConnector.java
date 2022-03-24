package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;

import java.util.List;
import java.util.Map;

public final class DQLOracleConnector extends AbstractDatabaseConnector {

    @Override
    protected String getTableSql(DatabaseConfig config) {
        return "SELECT TABLE_NAME,TABLE_TYPE FROM USER_TAB_COMMENTS";
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
    public List<Table> getTable(DatabaseConnectorMapper config) {
        return super.getDqlTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(DatabaseConnectorMapper connectorMapper, String tableName) {
        return super.getDqlMetaInfo(connectorMapper);
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
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.model.PageSql;

public final class PostgreSQLConnector extends AbstractDatabaseConnector {

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

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, String schema, String quotation, String queryFilterSql) {
        // 有过滤条件，走默认方式
        if (StringUtil.isNotBlank(queryFilterSql)) {
            return super.getQueryCountSql(commandConfig, schema, quotation, queryFilterSql);
        }

        // 从系统表查询
        final String table = commandConfig.getTable().getName();
        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        return String.format("SELECT N_LIVE_TUP FROM PG_STAT_USER_TABLES WHERE SCHEMANAME='%s' AND RELNAME='%s'", cfg.getSchema(), table);
    }
}
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;

import java.sql.Types;

public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    public PostgreSQLConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new PostgreSQLOtherValueMapper());
    }

    @Override
    protected String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        final String quotation = buildSqlWithQuotation();
        final String pk = config.getPk();
        StringBuilder querySql = new StringBuilder(config.getQuerySql());
        String queryFilter = config.getSqlBuilderConfig().getQueryFilter();
        if (StringUtil.isNotBlank(queryFilter)) {
            querySql.append(" AND ");
        } else {
            querySql.append(" WHERE ");
        }
        querySql.append(quotation).append(pk).append(quotation).append(" > ? ORDER BY ").append(quotation).append(pk).append(quotation).append(" LIMIT ?");
        return querySql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        final String quotation = buildSqlWithQuotation();
        final String pk = config.getPk();
        StringBuilder querySql = new StringBuilder(config.getQuerySql()).append(" ORDER BY ").append(quotation).append(pk).append(quotation).append(" LIMIT ?");
        return querySql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        Object cursor = config.getCursor();
        if (null == cursor) {
            return new Object[]{pageSize};
        }
        return new Object[]{cursor, pageSize};
    }

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, String schema, String quotation, String queryFilterSql) {
        final Table table = commandConfig.getTable();
        if (StringUtil.isNotBlank(queryFilterSql) || TableTypeEnum.isView(table.getType())) {
            return super.getQueryCountSql(commandConfig, schema, quotation, queryFilterSql);
        }

        // 从系统表查询
        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        return String.format("SELECT N_LIVE_TUP FROM PG_STAT_USER_TABLES WHERE SCHEMANAME='%s' AND RELNAME='%s'", cfg.getSchema(), table.getName());
    }

    @Override
    protected boolean enableCursor() {
        return true;
    }

}
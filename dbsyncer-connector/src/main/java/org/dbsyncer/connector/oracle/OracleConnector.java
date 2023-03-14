package org.dbsyncer.connector.oracle;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;

import java.sql.Types;

public final class OracleConnector extends AbstractDatabaseConnector {

    public OracleConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new OracleOtherValueMapper());
    }

    @Override
    public String getPageSql(PageSql config) {
        return DatabaseConstant.ORACLE_PAGE_SQL_START + config.getQuerySql() + DatabaseConstant.ORACLE_PAGE_SQL_END;
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{pageIndex * pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    protected String buildSqlFilterWithQuotation(String value) {
        if (StringUtil.isNotBlank(value)) {
            // 支持Oracle系统函数（to_char/to_date/to_timestamp/to_number）
            String val = value.toLowerCase();
            if (StringUtil.startsWith(val, "to_") && StringUtil.endsWith(val, ")")) {
                return "";
            }
        }
        return super.buildSqlFilterWithQuotation(value);
    }

    @Override
    protected String getValidationQuery() {
        return "select 1 from dual";
    }

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, String schema, String quotation, String queryFilterSql) {
        final Table table = commandConfig.getTable();
        if (StringUtil.isNotBlank(queryFilterSql) || TableTypeEnum.isView(table.getType())) {
            return super.getQueryCountSql(commandConfig, schema, quotation, queryFilterSql);
        }

        // 从系统表查询
        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        return String.format("SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'", cfg.getUsername().toUpperCase(), table.getName());
    }

}
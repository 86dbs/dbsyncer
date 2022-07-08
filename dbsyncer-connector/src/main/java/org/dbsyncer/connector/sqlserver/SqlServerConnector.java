package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;

import java.util.List;

public final class SqlServerConnector extends AbstractDatabaseConnector {

    @Override
    public List<Table> getTable(DatabaseConnectorMapper connectorMapper) {
        DatabaseConfig config = connectorMapper.getConfig();
        return super.getTable(connectorMapper, String.format("SELECT NAME FROM SYS.TABLES WHERE SCHEMA_ID = SCHEMA_ID('%s') AND IS_MS_SHIPPED = 0", config.getSchema()));
    }

    @Override
    public String getPageSql(PageSql config) {
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, config.getPk(), config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[] {(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, String schema, String quotation, String queryFilterSql) {
        // 有过滤条件，走默认方式
        if (StringUtil.isNotBlank(queryFilterSql)) {
            return super.getQueryCountSql(commandConfig, schema, quotation, queryFilterSql);
        }

        String table = commandConfig.getTable().getName();
        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        // 从存储过程查询（定时更新总数，可能存在误差）
        return String.format("SELECT ROWS FROM SYSINDEXES WHERE ID = OBJECT_ID('%s.%s') AND INDID IN (0, 1)", cfg.getSchema(), table);
    }
}
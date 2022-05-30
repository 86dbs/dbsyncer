package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SqlServerConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

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
        return new Object[]{(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        // 获取过滤SQL
        String queryFilterSql = this.getQueryFilterSql(commandConfig.getFilter());

        // 获取查询SQL
        Table table = commandConfig.getTable();
        Map<String, String> map = new HashMap<>();

        String query = ConnectorConstant.OPERTION_QUERY;
        map.put(query, this.buildSql(query, commandConfig, queryFilterSql));

        // 获取查询总数SQL
        StringBuilder queryCount = new StringBuilder();
        if (StringUtil.isNotBlank(queryFilterSql)) {
            queryCount.append("SELECT COUNT(*) FROM ").append(table.getName()).append(queryFilterSql);
        } else {
            DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
            // 从存储过程查询（定时更新总数，可能存在误差）
            queryCount.append("SELECT ROWS FROM SYSINDEXES WHERE ID = OBJECT_ID('").append(cfg.getSchema()).append(".").append(table.getName()).append(
                    "') AND INDID IN (0, 1)");
        }
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, queryCount.toString());
        return map;
    }

}
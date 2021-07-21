package org.dbsyncer.connector.sqlserver;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PageSqlConfig;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public final class SqlServerConnector extends AbstractDatabaseConnector implements SqlServer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected String getTableSql(DatabaseConfig config) {
        return "SELECT NAME FROM SYS.TABLES WHERE SCHEMA_ID = SCHEMA_ID('DBO') AND IS_MS_SHIPPED = 0";
    }

    @Override
    public String getPageSql(PageSqlConfig config) {
        if (StringUtils.isBlank(config.getPk())) {
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
        }
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
        map.put(query, this.buildSql(query, table, commandConfig.getOriginalTable(), queryFilterSql));

        // 获取查询总数SQL
        StringBuilder queryCount = new StringBuilder();
        if (StringUtils.isNotBlank(queryFilterSql)) {
            queryCount.append("SELECT COUNT(*) FROM ").append(table.getName()).append(queryFilterSql);
        } else {
            // 从存储过程查询（定时更新总数，可能存在误差）
            queryCount.append("SELECT ROWS FROM SYSINDEXES WHERE ID = OBJECT_ID('").append("DBO.").append(table.getName()).append(
                    "') AND INDID IN (0, 1)");
        }
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, queryCount.toString());
        return map;
    }

}
package org.dbsyncer.connector.oracle;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PageSqlConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.springframework.jdbc.core.JdbcTemplate;

public final class OracleConnector extends AbstractDatabaseConnector {

    @Override
    public boolean isAlive(ConnectorMapper connectorMapper) {
        JdbcTemplate jdbcTemplate = (JdbcTemplate) connectorMapper.getConnection();
        if(null != jdbcTemplate){
            Integer count = jdbcTemplate.queryForObject("select 1 from dual", Integer.class);
            return count > 0;
        }
        return false;
    }

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
        return new Object[]{pageIndex * pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    protected String buildSqlWithQuotation() {
        return "\"";
    }
}
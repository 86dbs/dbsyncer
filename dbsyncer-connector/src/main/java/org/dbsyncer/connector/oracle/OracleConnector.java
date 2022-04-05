package org.dbsyncer.connector.oracle;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PageSqlConfig;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class OracleConnector extends AbstractDatabaseConnector<DatabaseConfig> {

    @Override
    protected String getTableSql(DatabaseConfig config) {
        return "SELECT TABLE_NAME,TABLE_TYPE FROM USER_TAB_COMMENTS";
    }

    @Override
    public List<Table> getTable(DatabaseConnectorMapper connectorMapper) {
        String sql = getTableSql(connectorMapper.getConfig());
        List<Map<String, Object>> list = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(sql));
        if (!CollectionUtils.isEmpty(list)) {
            return list.stream().map(r -> new Table(r.get("TABLE_NAME").toString(), r.get("TABLE_TYPE").toString())).collect(Collectors.toList());
        }
        return Collections.EMPTY_LIST;
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

    @Override
    protected String getValidationQuery() {
        return "select 1 from dual";
    }
}
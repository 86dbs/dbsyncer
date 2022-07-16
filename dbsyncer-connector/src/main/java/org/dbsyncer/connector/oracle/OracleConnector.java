package org.dbsyncer.connector.oracle;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class OracleConnector extends AbstractDatabaseConnector {

    @Override
    public List<Table> getTable(DatabaseConnectorMapper connectorMapper) {
        final String sql = "SELECT TABLE_NAME,TABLE_TYPE FROM USER_TAB_COMMENTS";
        List<Map<String, Object>> list = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(sql));
        if (!CollectionUtils.isEmpty(list)) {
            return list.stream().map(r -> new Table(r.get("TABLE_NAME").toString(), r.get("TABLE_TYPE").toString())).collect(Collectors.toList());
        }
        return Collections.EMPTY_LIST;
    }

    @Override
    public String getPageSql(PageSql config) {
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

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, String schema, String quotation, String queryFilterSql) {
        // 有过滤条件，走默认方式
        if (StringUtil.isNotBlank(queryFilterSql)) {
            return super.getQueryCountSql(commandConfig, schema, quotation, queryFilterSql);
        }

        // 从系统表查询
        final String table = commandConfig.getTable().getName();
        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        return String.format("SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'", cfg.getUsername().toUpperCase(), table);
    }
}
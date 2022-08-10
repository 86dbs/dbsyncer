package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;

import java.util.LinkedList;
import java.util.List;

public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    @Override
    public List<Table> getTable(DatabaseConnectorMapper connectorMapper) {
        List<Table> list = new LinkedList<>();
        DatabaseConfig config = connectorMapper.getConfig();
        final String queryTableSql = String.format("SELECT TABLENAME FROM PG_TABLES WHERE SCHEMANAME ='%s'", config.getSchema());
        List<String> tableNames = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(queryTableSql, String.class));
        if (!CollectionUtils.isEmpty(tableNames)) {
            tableNames.forEach(name -> list.add(new Table(name, TableTypeEnum.TABLE.getCode())));
        }

        final String queryTableViewSql = String.format("SELECT VIEWNAME FROM PG_VIEWS WHERE SCHEMANAME ='%s'", config.getSchema());
        List<String> tableViewNames = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(queryTableViewSql, String.class));
        if (!CollectionUtils.isEmpty(tableViewNames)) {
            tableViewNames.forEach(name -> list.add(new Table(name, TableTypeEnum.VIEW.getCode())));
        }
        return list;
    }

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
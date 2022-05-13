package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.enums.TableTypeEnum;

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
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    protected String buildSqlWithQuotation() {
        return "\"";
    }
}

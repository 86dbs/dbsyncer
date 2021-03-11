package org.dbsyncer.connector.sqlserver;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SqlServerConnector extends AbstractDatabaseConnector implements SqlServer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected String getTablesSql(DatabaseConfig config) {
        return "select name from sysobjects where xtype='u'";
    }

    @Override
    public String getTableColumnSql(String querySQL) {
        return querySQL;
    }

    @Override
    public String getPageSql(String tableName, String pk, String querySQL) {
        if(StringUtils.isBlank(pk)){
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
        }
        // SqlServer 分页查询
        // sql> SELECT * FROM SD_USER ORDER BY USER.ID OFFSET(3-1) * 1000 ROWS FETCH NEXT 1000 ROWS ONLY
        return new StringBuilder(querySQL).append(DatabaseConstant.SQLSERVER_PAGE_SQL_START).append(tableName).append(".").append(pk).append(DatabaseConstant.SQLSERVER_PAGE_SQL_END).toString();
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

}
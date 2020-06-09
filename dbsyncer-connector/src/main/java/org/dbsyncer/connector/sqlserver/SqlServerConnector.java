package org.dbsyncer.connector.sqlserver;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SqlServerConnector extends AbstractDatabaseConnector implements SqlServer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String getMetaSql(DatabaseConfig config, String tableName) {
        return new StringBuilder().append("SELECT * FROM ").append(tableName).toString();
    }

    @Override
    protected String getQueryTablesSql(DatabaseConfig config) {
        return "";
    }

    @Override
    public String getPageSql(String tableName, String pk, String querySQL) {
        if(StringUtils.isBlank(pk)){
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
        }
        // SqlServer 分页查询
        // sql> SELECT * FROM SD_USER ORDER BY USER.ID OFFSET(3-1) * 1000 ROWS FETCH NEXT 1000 ROWS ONLY
        return new StringBuilder(querySQL).append(" ORDER BY ").append(tableName).append(".").append(pk).append(" OFFSET(?-1) * ? ROWS FETCH NEXT ? ROWS ONLY").toString();
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

}
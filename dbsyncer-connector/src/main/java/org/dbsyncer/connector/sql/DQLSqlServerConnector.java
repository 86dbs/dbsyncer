package org.dbsyncer.connector.sql;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public final class DQLSqlServerConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected String getTablesSql(DatabaseConfig config) {
        return "SELECT NAME FROM SYS.TABLES WHERE SCHEMA_ID = SCHEMA_ID('DBO')";
    }

    @Override
    public String getPageSql(String querySQL, String pk) {
        if(StringUtils.isBlank(pk)){
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
        }
        return new StringBuilder(querySQL).append(DatabaseConstant.SQLSERVER_PAGE_SQL_START).append(pk).append(DatabaseConstant.SQLSERVER_PAGE_SQL_END).toString();
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

    @Override
    public List<String> getTable(ConnectorConfig config) {
        return super.getDqlTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(ConnectorConfig config, String tableName) {
        return super.getDqlMetaInfo(config);
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return super.getDqlSourceCommand(commandConfig,false);
    }

}
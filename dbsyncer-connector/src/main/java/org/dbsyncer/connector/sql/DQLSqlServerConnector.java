package org.dbsyncer.connector.sql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PageSqlConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.sqlserver.SqlServerConnectorMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DQLSqlServerConnector extends AbstractDQLConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public ConnectorMapper connect(DatabaseConfig config) {
        try {
            return new SqlServerConnectorMapper(config);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public String getPageSql(PageSqlConfig config) {
        if (StringUtil.isBlank(config.getPk())) {
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
        }
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, config.getPk(), config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(int pageIndex, int pageSize) {
        return new Object[]{(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }

}
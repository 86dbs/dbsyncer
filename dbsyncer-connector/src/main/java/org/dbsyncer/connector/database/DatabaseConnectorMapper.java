package org.dbsyncer.connector.database;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;

public class DatabaseConnectorMapper implements ConnectorMapper<DatabaseConfig, Connection> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private DatabaseConfig config;

    public DatabaseConnectorMapper(DatabaseConfig config) {
        this.config = config;
    }

    public <T> T execute(HandleCallback callback) {
        Connection connection = null;
        try {
            connection = DatabaseUtil.getConnection(config);
            return (T) callback.apply(new DatabaseTemplate(connection));
        } catch (EmptyResultDataAccessException e) {
            throw e;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        } finally {
            DatabaseUtil.close(connection);
        }
    }

    @Override
    public DatabaseConfig getConfig() {
        return config;
    }

}
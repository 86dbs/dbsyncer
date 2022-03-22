package org.dbsyncer.connector.database;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConnectorMapper implements ConnectorMapper<DatabaseConfig, Connection> {
    protected DatabaseConfig config;
    protected DatabaseTemplate template;

    public DatabaseConnectorMapper(DatabaseConfig config) throws SQLException {
        this.config = config;
        template = new DatabaseTemplate(DatabaseUtil.getConnection(config));
    }

    public <T> T execute(HandleCallback callback) {
        try {
            return (T) callback.apply(template);
        } catch (EmptyResultDataAccessException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public DatabaseConfig getConfig() {
        return config;
    }

    @Override
    public Connection getConnection() {
        return template.getConnection();
    }

    @Override
    public void close() {
        DatabaseUtil.close(getConnection());
    }
}
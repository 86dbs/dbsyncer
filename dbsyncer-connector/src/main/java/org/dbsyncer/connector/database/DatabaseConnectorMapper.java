package org.dbsyncer.connector.database;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;

public final class DatabaseConnectorMapper implements ConnectorMapper<DatabaseConfig, Connection> {
    private DatabaseConfig config;
    private Connection connection;

    public DatabaseConnectorMapper(DatabaseConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }

    public <T> T execute(HandleCallback callback) {
        try {
            return (T) callback.apply(new DatabaseTemplate(connection));
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
        return connection;
    }
}
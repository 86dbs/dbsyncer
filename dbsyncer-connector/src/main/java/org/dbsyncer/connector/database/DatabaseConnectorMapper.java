package org.dbsyncer.connector.database;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.HandleCallback;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;

public class DatabaseConnectorMapper implements ConnectorMapper<Connection> {
    private ConnectorConfig config;
    private Connection connection;

    public DatabaseConnectorMapper(ConnectorConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }

    @Override
    public ConnectorConfig getConfig() {
        return config;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    public <T> T execute(HandleCallback callback) {
        try {
            return (T) callback.apply(new DatabaseTemplate(connection));
        } catch (EmptyResultDataAccessException e) {
            throw e;
        }catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
    }

}
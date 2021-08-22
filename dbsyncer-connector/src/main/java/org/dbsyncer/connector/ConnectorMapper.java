package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.database.DatabaseTemplate;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;

public class ConnectorMapper {
    protected ConnectorConfig config;
    protected Object connection;

    public ConnectorMapper(ConnectorConfig config, Object connection) {
        this.config = config;
        this.connection = connection;
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    public Object getConnection() {
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
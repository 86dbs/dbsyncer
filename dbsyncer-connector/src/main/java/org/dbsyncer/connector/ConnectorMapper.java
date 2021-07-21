package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.database.DatabaseTemplate;

import java.sql.Connection;

public class ConnectorMapper {
    protected ConnectorConfig config;
    protected Connection connection;

    public ConnectorMapper(ConnectorConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    public Connection getConnection() {
        return connection;
    }

    /**
     * 使用连接时加锁
     *
     * @param callback
     * @return
     */
    public <T> T execute(HandleCallback callback) {
        try {
            return (T) callback.apply(new DatabaseTemplate(connection));
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
    }

}
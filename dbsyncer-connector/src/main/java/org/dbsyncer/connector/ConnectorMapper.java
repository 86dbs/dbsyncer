package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;

public class ConnectorMapper {
    private ConnectorConfig config;
    private String          cacheKey;
    private Object          connection;

    public ConnectorMapper(ConnectorConfig config, String cacheKey, Object connection) {
        this.config = config;
        this.cacheKey = cacheKey;
        this.connection = connection;
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public Object getConnection() {
        return connection;
    }
}
package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;

public interface ConnectorMapper<K, V> {

    default ConnectorConfig getOriginalConfig() {
        return (ConnectorConfig) getConfig();
    }

    K getConfig();

    V getConnection();
}
package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;

public interface ConnectorMapper<C> {

    ConnectorConfig getConfig();

    C getConnection();
}
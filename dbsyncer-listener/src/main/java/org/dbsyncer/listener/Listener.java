package org.dbsyncer.listener;

import org.dbsyncer.connector.config.ConnectorConfig;

public interface Listener {

    Extractor createExtractor(ConnectorConfig connectorConfig);
}

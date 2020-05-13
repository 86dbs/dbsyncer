package org.dbsyncer.listener;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;

import java.util.Map;

public interface Listener {

    DefaultExtractor createExtractor(ConnectorConfig connectorConfig, ListenerConfig listenerConfig, Map<String, String> map);
}
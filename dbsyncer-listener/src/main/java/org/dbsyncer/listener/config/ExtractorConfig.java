package org.dbsyncer.listener.config;

import org.dbsyncer.connector.config.ConnectorConfig;

import java.util.List;
import java.util.Map;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-25 23:52
 */
public class ExtractorConfig {

    private ConnectorConfig connectorConfig;
    private ListenerConfig listenerConfig;
    private Map<String, String> map;
    private List<Map<String, String>> commands;
    private List<String> tableNames;

    public ExtractorConfig(ConnectorConfig connectorConfig, ListenerConfig listenerConfig, Map<String, String> map, List<Map<String, String>> commands, List<String> tableNames) {
        this.connectorConfig = connectorConfig;
        this.listenerConfig = listenerConfig;
        this.map = map;
        this.commands = commands;
        this.tableNames = tableNames;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public ListenerConfig getListenerConfig() {
        return listenerConfig;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public List<Map<String, String>> getCommands() {
        return commands;
    }

    public List<String> getTableNames() {
        return tableNames;
    }
}

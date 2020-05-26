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
    private List<TableCommandConfig> tableCommandConfig;

    /**
     * 抽取器配置
     *
     * @param connectorConfig 连接器配置
     * @param listenerConfig 监听配置
     * @param map 增量元信息
     * @param tableCommandConfig 映射关系
     */
    public ExtractorConfig(ConnectorConfig connectorConfig, ListenerConfig listenerConfig, Map<String, String> map, List<TableCommandConfig> tableCommandConfig) {
        this.connectorConfig = connectorConfig;
        this.listenerConfig = listenerConfig;
        this.map = map;
        this.tableCommandConfig = tableCommandConfig;
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

    public List<TableCommandConfig> getTableCommandConfig() {
        return tableCommandConfig;
    }
}

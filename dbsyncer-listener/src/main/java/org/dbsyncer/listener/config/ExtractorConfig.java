package org.dbsyncer.listener.config;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.connector.config.ConnectorConfig;

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
    private Event event;

    /**
     * 抽取器配置
     *
     * @param connectorConfig 连接器配置
     * @param listenerConfig 监听配置
     * @param map 增量元信息
     * @param event 监听器
     */
    public ExtractorConfig(ConnectorConfig connectorConfig, ListenerConfig listenerConfig, Map<String, String> map, Event event) {
        this.connectorConfig = connectorConfig;
        this.listenerConfig = listenerConfig;
        this.map = map;
        this.event = event;
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

    public Event getEvent() {
        return event;
    }
}
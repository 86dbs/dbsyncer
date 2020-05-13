package org.dbsyncer.listener;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 20:35
 */
public abstract class DefaultExtractor implements Extractor {

    private ConnectorConfig connectorConfig;
    private ListenerConfig listenerConfig;
    private Map<String, String> map;
    private List<Event> watcher;

    public void addListener(Event event) {
        if (null != event) {
            if (null == watcher) {
                watcher = new CopyOnWriteArrayList<>();
            }
            watcher.add(event);
        }
    }

    public void changedEvent(String event, Map<String, Object> before, Map<String, Object> after) {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.changedEvent(event, before, after));
        }
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ListenerConfig getListenerConfig() {
        return listenerConfig;
    }

    public void setListenerConfig(ListenerConfig listenerConfig) {
        this.listenerConfig = listenerConfig;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }
}
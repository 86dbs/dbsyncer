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
 * @Date 2020-05-25 22:35
 */
public abstract class AbstractExtractor implements Extractor {

    protected ConnectorConfig connectorConfig;
    protected ListenerConfig listenerConfig;
    protected Map<String, String> map;
    private List<Event> watcher;

    public void addListener(Event event) {
        if (null != event) {
            if (null == watcher) {
                watcher = new CopyOnWriteArrayList<>();
            }
            watcher.add(event);
        }
    }

    public void clearAllListener() {
        if (null != watcher) {
            watcher.clear();
            watcher = null;
        }
    }

    public void changedEvent(String tableName, String event, List<Object> before, List<Object> after) {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.changedEvent(tableName, event, before, after));
        }
    }

    public void flushEvent() {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.flushEvent(map));
        }
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public void setListenerConfig(ListenerConfig listenerConfig) {
        this.listenerConfig = listenerConfig;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }

}

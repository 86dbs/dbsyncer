package org.dbsyncer.listener;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.event.RowChangedEvent;
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

    @Override
    public void addListener(Event event) {
        if (null != event) {
            if (null == watcher) {
                watcher = new CopyOnWriteArrayList<>();
            }
            watcher.add(event);
        }
    }

    @Override
    public void clearAllListener() {
        if (null != watcher) {
            watcher.clear();
            watcher = null;
        }
    }

    @Override
    public void changedQuartzEvent(RowChangedEvent rowChangedEvent) {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.changedQuartzEvent(rowChangedEvent));
        }
    }

    @Override
    public void changedLogEvent(RowChangedEvent rowChangedEvent) {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.changedLogEvent(rowChangedEvent));
        }
    }

    @Override
    public void flushEvent() {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.flushEvent(map));
        }
    }

    @Override
    public void forceFlushEvent(){
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.forceFlushEvent(map));
        }
    }

    @Override
    public void errorEvent(Exception e) {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.errorEvent(e));
        }
    }

    @Override
    public void interruptException(Exception e) {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.interruptException(e));
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
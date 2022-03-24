package org.dbsyncer.listener;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-25 22:35
 */
public abstract class AbstractExtractor implements Extractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected Executor taskExecutor;
    protected ConnectorFactory connectorFactory;
    protected ScheduledTaskService scheduledTaskService;
    protected ConnectorConfig connectorConfig;
    protected ListenerConfig listenerConfig;
    protected Map<String, String> snapshot;
    protected Set<String> filterTable;
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
    public void changedEvent(RowChangedEvent event) {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.changedEvent(event));
        }
    }

    @Override
    public void flushEvent() {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.flushEvent(snapshot));
        }
    }

    @Override
    public void forceFlushEvent() {
        if (!CollectionUtils.isEmpty(watcher)) {
            logger.info("Force flush:{}", snapshot);
            watcher.forEach(w -> w.forceFlushEvent(snapshot));
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

    protected void asyncSendRowChangedEvent(RowChangedEvent event) {
        taskExecutor.execute(() -> changedEvent(event));
    }

    public void setTaskExecutor(Executor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public void setConnectorFactory(ConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    public void setScheduledTaskService(ScheduledTaskService scheduledTaskService) {
        this.scheduledTaskService = scheduledTaskService;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public void setListenerConfig(ListenerConfig listenerConfig) {
        this.listenerConfig = listenerConfig;
    }

    public void setSnapshot(Map<String, String> snapshot) {
        this.snapshot = snapshot;
    }

    public void setFilterTable(Set<String> filterTable) {
        this.filterTable = filterTable;
    }
}
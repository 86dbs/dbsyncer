package org.dbsyncer.listener;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.listener.config.ListenerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-25 22:35
 */
public abstract class AbstractExtractor implements Extractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected ConnectorFactory connectorFactory;
    protected ScheduledTaskService scheduledTaskService;
    protected AbstractConnectorConfig connectorConfig;
    protected ListenerConfig listenerConfig;
    protected Set<String> filterTable;
    protected List<Table> sourceTable;
    protected Map<String, String> snapshot;
    protected String metaId;
    protected Event watcher;

    @Override
    public void register(Event event) {
        watcher = event;
    }

    @Override
    public void changedEvent(RowChangedEvent event) {
        if (null != event) {
            switch (event.getEvent()) {
                case ConnectorConstant.OPERTION_UPDATE:
                    // 是否支持监听修改事件
                    processEvent(!listenerConfig.isBanUpdate(), event);
                    break;
                case ConnectorConstant.OPERTION_INSERT:
                    // 是否支持监听新增事件
                    processEvent(!listenerConfig.isBanInsert(), event);
                    break;
                case ConnectorConstant.OPERTION_DELETE:
                    // 是否支持监听删除事件
                    processEvent(!listenerConfig.isBanDelete(), event);
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void flushEvent() {
        watcher.flushEvent(snapshot);
    }

    @Override
    public void forceFlushEvent() {
        logger.info("Force flush:{}", snapshot);
        watcher.forceFlushEvent(snapshot);
    }

    @Override
    public void errorEvent(Exception e) {
        watcher.errorEvent(e);
    }

    @Override
    public void interruptException(Exception e) {
        watcher.interruptException(e);
    }

    protected void sleepInMills(long timeout) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        }
    }

    /**
     * 如果允许监听该事件，则触发事件通知
     *
     * @param permitEvent
     * @param event
     */
    private void processEvent(boolean permitEvent, RowChangedEvent event) {
        if (permitEvent) {
            watcher.changedEvent(event);
        }
    }

    public void setConnectorFactory(ConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    public void setScheduledTaskService(ScheduledTaskService scheduledTaskService) {
        this.scheduledTaskService = scheduledTaskService;
    }

    public void setConnectorConfig(AbstractConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public void setListenerConfig(ListenerConfig listenerConfig) {
        this.listenerConfig = listenerConfig;
    }

    public void setFilterTable(Set<String> filterTable) {
        this.filterTable = filterTable;
    }

    public AbstractExtractor setSourceTable(List<Table> sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public void setSnapshot(Map<String, String> snapshot) {
        this.snapshot = snapshot;
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }
}
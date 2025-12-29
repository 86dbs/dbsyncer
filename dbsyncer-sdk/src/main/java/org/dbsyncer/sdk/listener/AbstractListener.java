/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.config.ListenerConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-25 22:35
 */
public abstract class AbstractListener<C extends ConnectorInstance> implements Listener {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected ConnectorInstance connectorInstance;
    protected ConnectorInstance targetConnectorInstance;
    protected ConnectorService connectorService;
    protected ScheduledTaskService scheduledTaskService;
    protected ConnectorConfig connectorConfig;
    protected ListenerConfig listenerConfig;
    protected Set<String> filterTable;
    protected List<Table> sourceTable;
    protected Map<String, String> snapshot;
    protected String metaId;
    private Watcher watcher;
    // 保存上次持久化的 snapshot，用于判断是否发生变化
    private Map<String, String> lastFlushedSnapshot;

    @Override
    public void register(Watcher watcher) {
        this.watcher = watcher;
    }

    @Override
    public void changeEventBefore(QuartzListenerContext context) {
        watcher.changeEventBefore(context);
    }

    @Override
    public void changeEvent(ChangedEvent event) {
        if (null != event) {
            switch (event.getEvent()) {
                case ConnectorConstant.OPERTION_UPDATE:
                    // 是否支持监听修改事件
                    processEvent(listenerConfig.isEnableUpdate(), event);
                    break;
                case ConnectorConstant.OPERTION_INSERT:
                    // 是否支持监听新增事件
                    processEvent(listenerConfig.isEnableInsert(), event);
                    break;
                case ConnectorConstant.OPERTION_DELETE:
                    // 是否支持监听删除事件
                    processEvent(listenerConfig.isEnableDelete(), event);
                    break;
                case ConnectorConstant.OPERTION_ALTER:
                    // 表结构变更事件：检查 DDL 开关
                    processEvent(listenerConfig.isEnableDDL(), event);
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        // nothing to do
    }

    @Override
    public void flushEvent() throws Exception {
        if (CollectionUtils.isEmpty(snapshot)) {
            return;
        }
        
        // 判断 snapshot 是否发生变化，只有变化时才持久化
        if (hasSnapshotChanged(snapshot, lastFlushedSnapshot)) {
            logger.info("snapshot changed, flushing: {}", snapshot);
            watcher.flushEvent(snapshot);
            // 更新上次持久化的 snapshot（深拷贝）
            lastFlushedSnapshot = new HashMap<>(snapshot);
        }
    }
    
    /**
     * 判断 snapshot 是否发生变化
     * 
     * @param current 当前 snapshot
     * @param last 上次持久化的 snapshot
     * @return true 如果发生变化，false 如果未变化
     */
    private boolean hasSnapshotChanged(Map<String, String> current, Map<String, String> last) {
        if (last == null || last.isEmpty()) {
            // 首次持久化：如果当前 snapshot 不为空，则需要持久化
            return !current.isEmpty();
        }
        
        // 大小不同，肯定有变化
        if (current.size() != last.size()) {
            return true;
        }
        
        // 比较所有 key-value
        for (Map.Entry<String, String> entry : current.entrySet()) {
            String lastValue = last.get(entry.getKey());
            if (!Objects.equals(entry.getValue(), lastValue)) {
                return true;
            }
        }
        
        return false;
    }

    @Override
    public void forceFlushEvent() throws Exception {
        if (!CollectionUtils.isEmpty(snapshot)) {
            logger.info("snapshot：{}", snapshot);
            watcher.flushEvent(snapshot);
        }
    }

    @Override
    public void errorEvent(Exception e) {
        watcher.errorEvent(e);
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
    private void processEvent(boolean permitEvent, ChangedEvent event) {
        if (permitEvent) {
            watcher.changeEvent(event);
        }
    }

    public void setConnectorInstance(ConnectorInstance connectorInstance) {
        this.connectorInstance = connectorInstance;
    }

    public C getConnectorInstance() {
        return (C) connectorInstance;
    }

    public void setTargetConnectorInstance(ConnectorInstance targetConnectorInstance) {
        this.targetConnectorInstance = targetConnectorInstance;
    }

    public void setConnectorService(ConnectorService connectorService) {
        this.connectorService = connectorService;
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

    public void setFilterTable(Set<String> filterTable) {
        this.filterTable = filterTable;
    }

    public AbstractListener setSourceTable(List<Table> sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public void setSnapshot(Map<String, String> snapshot) {
        this.snapshot = snapshot;
        // 初始化时，如果 snapshot 不为空，也初始化 lastFlushedSnapshot
        if (this.lastFlushedSnapshot == null && !CollectionUtils.isEmpty(snapshot)) {
            this.lastFlushedSnapshot = new HashMap<>(snapshot);
        }
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

}
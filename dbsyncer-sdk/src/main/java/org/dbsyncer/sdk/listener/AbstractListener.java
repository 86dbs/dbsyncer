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

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
    // 待持久化的快照点（异步批量持久化）
    private volatile Map<String, String> pendingSnapshot = null;
    // 异步持久化执行器
    private ScheduledExecutorService flushExecutor;

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
    public void init() {
        // 初始化异步持久化执行器
        initFlushExecutor();
    }

    /**
     * 初始化异步持久化执行器
     * 在 Listener 初始化时调用（通过 init() 方法）
     */
    private void initFlushExecutor() {
        if (flushExecutor == null) {
            flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "snapshot-flush-" + metaId);
                t.setDaemon(true);
                return t;
            });
            // 每 3 秒检查并写入最新快照
            flushExecutor.scheduleWithFixedDelay(() -> {
                Map<String, String> snapshot = pendingSnapshot;
                // 如果 pendingSnapshot 不为空且与上次持久化的不同，则持久化
                // 使用 Objects.equals 比较，避免重复持久化相同的快照点
                if (snapshot != null && !Objects.equals(snapshot, lastFlushedSnapshot)) {
                    try {
                        logger.info("snapshot changed, flushing: {}", snapshot);
                        watcher.flushEvent(snapshot);
                        lastFlushedSnapshot = new HashMap<>(snapshot);
                        pendingSnapshot = null;
                    } catch (Exception e) {
                        logger.error("异步持久化失败", e);
                    }
                }
            }, 3, 3, TimeUnit.SECONDS);
        }
    }

    @Override
    public void flushEvent() throws Exception {
        if (CollectionUtils.isEmpty(snapshot)) {
            return;
        }
        
        pendingSnapshot = new HashMap<>(snapshot);
    }
    
    /**
     * 系统关闭时立即持久化最新快照点
     */
    @PreDestroy
    public void destroy() {
        if (flushExecutor != null) {
            flushExecutor.shutdown();
            try {
                // 等待正在执行的任务完成
                if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    flushExecutor.shutdownNow();
                }
                // 系统关闭时立即持久化最新快照
                Map<String, String> snapshot = pendingSnapshot;
                if (snapshot != null && !Objects.equals(snapshot, lastFlushedSnapshot)) {
                    try {
                        watcher.flushEvent(snapshot);
                    } catch (Exception e) {
                        logger.error("系统关闭时持久化快照失败", e);
                    }
                }
            } catch (InterruptedException e) {
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
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
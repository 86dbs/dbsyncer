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
    // 标记是否有任务数据在处理（从 ROW 事件进入队列到数据同步完成）
    // 使用 AtomicInteger 计数器，支持多线程并发场景
    private final java.util.concurrent.atomic.AtomicInteger pendingTaskDataCount = new java.util.concurrent.atomic.AtomicInteger(0);

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
        
        // 任务事件：直接更新 pendingSnapshot（覆盖旧值，因为这是最新的任务数据快照）
        // 非任务事件：如果没有任务数据在处理，也直接更新 pendingSnapshot（覆盖旧值，因为这是最新的非任务事件快照）
        // 注意：非任务事件在 XID 事件处理时已经检查了 hasPendingTaskData，所以这里可以直接更新
        pendingSnapshot = new HashMap<>(snapshot);
    }
    
    /**
     * 增加任务数据计数（ROW 事件进入队列时调用）
     */
    public void incrementPendingTaskData() {
        pendingTaskDataCount.incrementAndGet();
    }
    
    /**
     * 减少任务数据计数（数据同步完成时调用）
     * 只有在计数 > 0 时才减少，避免 DDL/SCAN 等非 ROW 事件导致计数变成负数
     */
    public void decrementPendingTaskData() {
        // 使用 updateAndGet 确保原子性：只有在计数 > 0 时才减少
        pendingTaskDataCount.updateAndGet(count -> count > 0 ? count - 1 : 0);
    }
    
    /**
     * 获取是否有任务数据在处理
     * 
     * @return true 表示有任务数据在处理，false 表示没有
     */
    public boolean hasPendingTaskData() {
        return pendingTaskDataCount.get() > 0;
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
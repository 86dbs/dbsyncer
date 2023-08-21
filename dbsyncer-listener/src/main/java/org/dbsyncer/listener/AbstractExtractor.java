package org.dbsyncer.listener;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.listener.config.ListenerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private Event consumer;
    private BlockingQueue<RowChangedEvent> queue;
    private Thread consumerThread;
    private volatile boolean enableConsumerThread;
    private Lock lock = new ReentrantLock();
    private Condition isFull;
    private final Duration pollInterval = Duration.of(500, ChronoUnit.MILLIS);
    private static final int FLUSH_DELAYED_SECONDS = 30;
    private long updateTime;


    @Override
    public void start() {
        this.lock = new ReentrantLock();
        this.isFull = lock.newCondition();
        enableConsumerThread = true;
        consumerThread = new Thread(() -> {
            while (enableConsumerThread) {
                try {
                    // 取走BlockingQueue里排在首位的对象,若BlockingQueue为空,阻断进入等待状态直到Blocking有新的对象被加入为止
                    RowChangedEvent event = queue.take();
                    if (null != event) {
                        // TODO 待优化多表并行模型
                        consumer.changedEvent(event);
                        // 更新增量点
                        refreshEvent(event);
                        updateTime = Instant.now().toEpochMilli();
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        consumerThread.setName(new StringBuilder("extractor-consumer-").append(metaId).toString());
        consumerThread.setDaemon(false);
        consumerThread.start();
    }

    @Override
    public void close() {
        enableConsumerThread = false;
        if (consumerThread != null && !enableConsumerThread) {
            consumerThread.interrupt();
        }
    }

    @Override
    public void register(Event consumer) {
        this.consumer = consumer;
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
        // 30s内更新，执行写入
        if (updateTime > 0 && updateTime > Timestamp.valueOf(LocalDateTime.now().minusSeconds(FLUSH_DELAYED_SECONDS)).getTime()) {
            forceFlushEvent();
        }
    }


    @Override
    public void forceFlushEvent() {
        if (!CollectionUtils.isEmpty(snapshot)) {
            consumer.flushEvent(snapshot);
        }
    }

    @Override
    public void errorEvent(Exception e) {
        consumer.errorEvent(e);
    }

    /**
     * 更新增量点
     *
     * @param event
     */
    protected void refreshEvent(RowChangedEvent event) {
        // nothing to do
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
        if (!permitEvent) {
            return;
        }

        boolean lock = false;
        try {
            lock = this.lock.tryLock();
            if (lock) {
                if (!queue.offer(event)) {
                    // 容量上限，阻塞重试
                    while (!queue.offer(event)) {
                        try {
                            this.isFull.await(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
            }
        } finally {
            if (lock) {
                this.lock.unlock();
            }
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

    public void setQueue(BlockingQueue<RowChangedEvent> queue) {
        this.queue = queue;
    }
}
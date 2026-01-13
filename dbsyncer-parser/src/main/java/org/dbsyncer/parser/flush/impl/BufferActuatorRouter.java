/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.TableGroupBufferConfig;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.spi.TableGroupBufferActuatorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 缓存执行器路由
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
@Component
public final class BufferActuatorRouter implements DisposableBean {

    @Autowired
    private ProfileComponent profileComponent;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TableGroupBufferConfig tableGroupBufferConfig;

    @Resource
    private TableGroupBufferActuatorService tableGroupBufferActuatorService;

    @Resource
    private GeneralBufferActuator generalBufferActuator;

    /**
     * 驱动缓存执行路由列表
     */
    private final Map<String, Map<String, TableGroupBufferActuator>> router = new ConcurrentHashMap<>();

    /**
     * 标记每个 metaId 是否有任务在处理
     */
    private final Map<String, Boolean> pendingTaskMap = new ConcurrentHashMap<>();

    /**
     * 刷新监听器偏移量
     *
     * @param offset 偏移量
     */
    public void refreshOffset(ChangedOffset offset) {
        if (offset == null) {
            return;
        }

        Meta meta = profileComponent.getMeta(offset.getMetaId());
        assert meta != null;

        Listener listener = meta.getListener();
        if (listener == null) {
            return;
        }

        try {
            // refreshEvent() 已经直接更新了 pendingSnapshot，不需要再调用 flushEvent()
            // 原因：任务事件携带的XID快照不应该能够修改，refreshEvent() 直接使用任务携带的位置更新 pendingSnapshot
            listener.refreshEvent(offset);
            // 数据同步完成，检查队列是否为空，如果为空则清除 pending 状态
            long queueSize = getQueueSize().get();
            if (queueSize == 0) {
                pendingTaskMap.remove(offset.getMetaId());
                if (logger.isDebugEnabled()) {
                    logger.debug("---- finished sync, queue is empty, cleared pending task");
                }
            }
        } catch (Exception e) {
            logger.error("刷新监听器偏移量失败: metaId={}, error={}", offset.getMetaId(), e.getMessage(), e);
        }
    }

    public void execute(String metaId, ChangedEvent event) {
        // 事件进入队列时，标记为 pending 状态
        pendingTaskMap.put(metaId, true);
        event.getChangedOffset().setMetaId(metaId);
        router.compute(metaId, (k, processor) -> {
            if (processor == null) {
                offer(generalBufferActuator, event);
                return null;
            }

            processor.compute(event.getSourceTableName(), (x, actuator) -> {
                if (actuator == null) {
                    offer(generalBufferActuator, event);
                    return null;
                }
                offer(actuator, event);
                return actuator;
            });
            return processor;
        });
    }

    public void bind(String metaId, List<TableGroup> tableGroups) {
        router.computeIfAbsent(metaId, k -> {
            Map<String, TableGroupBufferActuator> processor = new ConcurrentHashMap<>();
            for (TableGroup tableGroup : tableGroups) {
                // 超过执行器上限
                if (processor.size() >= profileComponent.getSystemConfig().getMaxBufferActuatorSize()) {
                    logger.warn("Not allowed more than table processor limited size:{}", profileComponent.getSystemConfig().getMaxBufferActuatorSize());
                    break;
                }
                final String tableName = tableGroup.getSourceTable().getName();
                processor.computeIfAbsent(tableName, name -> {
                    TableGroupBufferActuator newBufferActuator = null;
                    try {
                        newBufferActuator = (TableGroupBufferActuator) tableGroupBufferActuatorService.clone();
                        newBufferActuator.setTableName(name);
                        newBufferActuator.buildConfig();
                    } catch (CloneNotSupportedException ex) {
                        logger.error(ex.getMessage(), ex);
                    }
                    return newBufferActuator;
                });
            }
            return processor;
        });
    }

    public void unbind(String metaId) {
        router.computeIfPresent(metaId, (k, processor) -> {
            processor.values().forEach(TableGroupBufferActuator::stop);
            return null;
        });
        // 清除 pending 状态
        pendingTaskMap.remove(metaId);
    }

    private void offer(AbstractBufferActuator actuator, ChangedEvent event) {
        if (ChangedEventTypeEnum.isDDL(event.getType())) {
            WriterRequest request = new WriterRequest(event);
            // DDL事件，阻塞等待队列消费完成
            while (actuator.isRunning(request)) {
                if (actuator.getQueue().isEmpty()) {
                    actuator.offer(request);
                    return;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException ex) {
                    logger.error(ex.getMessage(), ex);
                }
            }
        }
        actuator.offer(new WriterRequest(event));
    }

    @Override
    public void destroy() {
        router.values().forEach(map -> map.values().forEach(TableGroupBufferActuator::stop));
        router.clear();
    }

    public AtomicLong getQueueSize() {
        AtomicLong total = new AtomicLong();
        router.values().forEach(map -> map.values().forEach(actuator -> total.addAndGet(actuator.getQueue().size())));
        return total;
    }

    public AtomicLong getQueueCapacity() {
        AtomicLong total = new AtomicLong();
        router.values().forEach(map -> map.values().forEach(actuator -> total.addAndGet(actuator.getQueueCapacity())));
        return total;
    }

    public Map<String, Map<String, TableGroupBufferActuator>> getRouter() {
        return Collections.unmodifiableMap(router);
    }

    /**
     * 检查是否有任务在处理
     */
    public boolean hasPendingTask(String metaId) {
        return Boolean.TRUE.equals(pendingTaskMap.get(metaId));
    }

}
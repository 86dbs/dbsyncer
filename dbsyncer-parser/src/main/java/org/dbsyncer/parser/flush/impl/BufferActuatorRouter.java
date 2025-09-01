/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.common.config.TableGroupBufferConfig;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.listener.ChangedEvent;
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
    private TableGroupBufferActuator tableGroupBufferActuator;

    @Resource
    private GeneralBufferActuator generalBufferActuator;

    /**
     * 驱动缓存执行路由列表
     */
    private final Map<String, Map<String, TableGroupBufferActuator>> router = new ConcurrentHashMap<>();
    
    /**
     * 刷新监听器偏移量
     *
     * @param offset 偏移量
     */
    public void refreshOffset(ChangedOffset offset) {
        if (offset != null) {
            Meta meta = profileComponent.getMeta(offset.getMetaId());
            if (meta != null) {
                Listener listener = meta.getListener();
                if (listener != null) {
                    try {
                        listener.refreshEvent(offset);
                    } catch (Exception e) {
                        logger.error("刷新监听器偏移量失败: metaId={}, error={}", offset.getMetaId(), e.getMessage(), e);
                    }
                }
            }
        }
    }

    public void execute(String metaId, ChangedEvent event) {
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
                if (processor.size() >= tableGroupBufferConfig.getMaxBufferActuatorSize()) {
                    logger.warn("Not allowed more than table processor limited size.  maxBufferActuatorSize:{}", tableGroupBufferConfig.getMaxBufferActuatorSize());
                    break;
                }
                final String tableName = tableGroup.getSourceTable().getName();
                processor.computeIfAbsent(tableName, name -> {
                    TableGroupBufferActuator newBufferActuator = null;
                    try {
                        newBufferActuator = (TableGroupBufferActuator) tableGroupBufferActuator.clone();
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

}
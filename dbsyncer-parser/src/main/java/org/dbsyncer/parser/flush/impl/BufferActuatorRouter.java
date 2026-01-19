/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.GeneralBufferConfig;
import org.dbsyncer.common.metric.TimeRegistry;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupContext;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
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

    @Resource
    private GeneralBufferConfig generalBufferConfig;

    @Resource
    private Executor generalExecutor;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private DDLParser ddlParser;

    @Resource
    private TableGroupContext tableGroupContext;

    @Resource
    private LogService logService;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private TimeRegistry timeRegistry;

    /**
     * 每个 meta 独享的执行器
     */
    private final Map<String, MetaBufferActuator> metaActuatorMap = new ConcurrentHashMap<>();



    public void execute(String metaId, ChangedEvent event) {
        // 确保 ChangedOffset 不为 null，并设置 metaId
        if (event.getChangedOffset() == null) {
            throw new IllegalArgumentException("ChangedEvent.getChangedOffset() 不能为 null, event=" + event);
        }
        event.getChangedOffset().setMetaId(metaId);

        // 获取或创建该 meta 的执行器
        MetaBufferActuator actuator = getOrCreateActuator(metaId);

        // 所有事件都进入该 meta 的队列（offer 方法会自动处理 DDL 阻塞逻辑和 pending 状态）
        actuator.offer(new WriterRequest(event));
    }

    public void unbind(String metaId) {
        metaActuatorMap.computeIfPresent(metaId, (k, actuator) -> {
            actuator.stop(); // 停止该 meta 的执行器（stop 方法会自动清除 pending 状态）
            return null; // 从 map 中移除
        });
    }


    @Override
    public void destroy() {
        metaActuatorMap.values().forEach(MetaBufferActuator::stop); // 停止所有 MetaBufferActuator
        metaActuatorMap.clear(); // 清理 map
    }

    public AtomicLong getQueueSize() {
        AtomicLong total = new AtomicLong();
        metaActuatorMap.values().forEach(actuator -> total.addAndGet(actuator.getQueue().size()));
        return total;
    }

    public AtomicLong getQueueCapacity() {
        AtomicLong total = new AtomicLong();
        metaActuatorMap.values().forEach(actuator -> total.addAndGet(actuator.getQueueCapacity()));
        return total;
    }

    public Map<String, MetaBufferActuator> getMetaActuatorMap() {
        return Collections.unmodifiableMap(metaActuatorMap);
    }

    /**
     * 检查指定 meta 是否有任务在处理
     */
    public boolean hasPendingTask(String metaId) {
        MetaBufferActuator actuator = metaActuatorMap.get(metaId);
        return actuator != null && actuator.hasPendingTask();
    }

    /**
     * 获取或创建该 meta 的执行器
     *
     * @param metaId 元数据ID
     * @return MetaBufferActuator 实例
     */
    public MetaBufferActuator getOrCreateActuator(String metaId) {
        return metaActuatorMap.computeIfAbsent(metaId, k -> {
            MetaBufferActuator newActuator = createMetaActuator(metaId);
            newActuator.buildConfig();
            return newActuator;
        });
    }

    /**
     * 创建 MetaBufferActuator 实例（工厂方法）
     */
    private MetaBufferActuator createMetaActuator(String metaId) {
        MetaBufferActuator actuator = new MetaBufferActuator();
        actuator.setMetaId(metaId);
        // 手动注入所有依赖
        actuator.setGeneralBufferConfig(generalBufferConfig);
        actuator.setGeneralExecutor(generalExecutor);
        actuator.setConnectorFactory(connectorFactory);
        actuator.setProfileComponent(profileComponent);
        actuator.setPluginFactory(pluginFactory);
        actuator.setFlushStrategy(flushStrategy);
        actuator.setDdlParser(ddlParser);
        actuator.setTableGroupContext(tableGroupContext);
        actuator.setLogService(logService);
        // 注入 AbstractBufferActuator 需要的依赖
        actuator.setScheduledTaskService(scheduledTaskService);
        actuator.setTimeRegistry(timeRegistry);
        return actuator;
    }

}
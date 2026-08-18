/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.flush;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.impl.GeneralBufferActuator;
import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.model.BufferActuatorMetric;
import org.dbsyncer.sdk.spi.BufferActuatorRouterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 增量缓存执行器路由基类：共用 trace、DDL 入队与监控快照组装。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-12
 */
public abstract class AbstractBufferActuatorRouter implements BufferActuatorRouterService, DisposableBean {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    protected ProfileComponent profileComponent;

    @Resource
    protected GeneralBufferActuator generalBufferActuator;

    /**
     * 打印增量 trace。
     *
     * @param event 变更事件
     */
    protected void printTraceInfo(ChangedEvent event) {
        if (profileComponent.getSystemConfig() != null && profileComponent.getSystemConfig().isEnablePrintTraceInfo()) {
            event.setTraceId(UUIDUtil.getUUID().toLowerCase());
            logger.info("traceId:{}, tableName:{}, event:{}, offset:{}, row:{}", event.getTraceId(), event.getSourceTableName(),
                    event.getEvent(), JsonUtil.objToJson(event.getChangedOffset()), event.getChangedRow());
        }
    }

    /**
     * 投递到指定执行器；DDL 会等待目标执行器空闲（队列空且无 in-flight process）后再入队。
     * <p>等待被中断时恢复中断标记并仍尝试入队，避免 DDL 丢失。
     *
     * @param actuator 目标执行器
     * @param event    变更事件
     */
    protected void offer(AbstractBufferActuator actuator, ChangedEvent event) {
        if (ChangedEventTypeEnum.isDDL(event.getType())) {
            WriterRequest request = new WriterRequest(event);
            while (actuator.isRunning(request)) {
                if (actuator.isIdle()) {
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

    /**
     * 组装单个执行器监控快照。
     *
     * @param metaId   驱动 Meta ID
     * @param name     展示名
     * @param actuator 执行器
     * @return 快照
     */
    protected BufferActuatorMetric toMetric(String metaId, String name, AbstractBufferActuator actuator) {
        BufferActuatorMetric metric = new BufferActuatorMetric();
        metric.setMetaId(metaId);
        metric.setName(name);
        if (actuator == null) {
            return metric;
        }
        if (actuator.getQueue() != null) {
            metric.setQueueSize(actuator.getQueue().size());
        }
        metric.setQueueCapacity(actuator.getQueueCapacity());
        Executor executor = actuator.getExecutor();
        if (executor instanceof ThreadPoolTaskExecutor) {
            ThreadPoolExecutor pool = ((ThreadPoolTaskExecutor) executor).getThreadPoolExecutor();
            metric.setActiveCount(pool.getActiveCount());
            metric.setMaxPoolSize(pool.getMaximumPoolSize());
            metric.setCompletedTaskCount(pool.getCompletedTaskCount());
        }
        return metric;
    }

    /**
     * 停止表/管道执行器。
     *
     * @param actuator 执行器
     */
    protected void stopActuator(TableGroupBufferActuator actuator) {
        if (actuator != null) {
            actuator.stop();
        }
    }

    /**
     * 该驱动已绑定的执行器（不含通用执行器）。
     *
     * @param metaId 驱动 Meta ID
     * @return 执行器列表
     */
    protected abstract List<AbstractBufferActuator> listBoundActuators(String metaId);

    @Override
    public boolean drainAndAwaitIdle(String metaId, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            if (isBoundIdle(metaId) && generalBufferActuator.isIdle()) {
                return true;
            }
            if (System.currentTimeMillis() >= deadline) {
                logger.warn("drain timeout, metaId={}, timeoutMs={}", metaId, timeoutMs);
                return false;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(e.getMessage(), e);
                return false;
            }
        }
    }

    private boolean isBoundIdle(String metaId) {
        List<AbstractBufferActuator> actuators = listBoundActuators(metaId);
        if (actuators == null || actuators.isEmpty()) {
            return true;
        }
        for (AbstractBufferActuator actuator : actuators) {
            if (actuator != null && !actuator.isIdle()) {
                return false;
            }
        }
        return true;
    }
}

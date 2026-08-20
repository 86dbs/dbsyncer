/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.model.BufferActuatorMetric;

import java.util.List;

/**
 * 增量缓存执行器路由 SPI。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-12
 */
public interface BufferActuatorRouterService {

    /**
     * 绑定同步任务的增量执行器。
     *
     * @param metaId           同步任务 Meta ID
     * @param sourceTableNames 源表名列表
     * @param channelSize      执行器数
     */
    void bind(String metaId, List<String> sourceTableNames, int channelSize);

    /**
     * 解绑并停止该同步任务下的执行器。
     *
     * @param metaId 同步任务 Meta ID
     */
    void unbind(String metaId);

    /**
     * 投递增量变更事件。
     *
     * @param metaId 同步任务 Meta ID
     * @param event  变更事件
     */
    void execute(String metaId, ChangedEvent event);

    /**
     * 路由内所有执行器队列堆积总数（不含通用执行器）。
     *
     * @return 堆积数
     */
    long getQueueSize();

    /**
     * 路由内所有执行器队列容量总和（不含通用执行器）。
     *
     * @return 容量
     */
    long getQueueCapacity();

    /**
     * 执行器监控快照。
     *
     * @return 快照列表，无执行器时返回空列表
     */
    List<BufferActuatorMetric> listMetrics();

    /**
     * 排空指定同步任务的写队列直至空闲或超时。
     *
     * @param metaId    同步任务Meta ID
     * @param timeoutMs 超时毫秒
     * @return true 已空闲；false 超时仍有积压
     */
    boolean drainAndAwaitIdle(String metaId, long timeoutMs);
}

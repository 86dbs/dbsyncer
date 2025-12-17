/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.model;

import org.dbsyncer.biz.vo.HistoryStackVo;
import org.dbsyncer.biz.vo.MetricResponseVo;

import java.util.List;

public class AppReportMetric {

    /**
     * 待处理数
     */
    private long queueUp;

    /**
     * 队列长度
     */
    private long queueCapacity;

    /**
     * 持久化待处理数
     */
    private long storageQueueUp;

    /**
     * 持久化队列长度
     */
    private long storageQueueCapacity;

    /**
     * 统计执行器TPS
     */
    private HistoryStackVo tps;

    private HistoryStackVo cpu;

    private HistoryStackVo memory;

    private List<MetricResponseVo> metrics;

    public long getQueueUp() {
        return queueUp;
    }

    public void setQueueUp(long queueUp) {
        this.queueUp = queueUp;
    }

    public long getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(long queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public long getStorageQueueUp() {
        return storageQueueUp;
    }

    public void setStorageQueueUp(long storageQueueUp) {
        this.storageQueueUp = storageQueueUp;
    }

    public long getStorageQueueCapacity() {
        return storageQueueCapacity;
    }

    public void setStorageQueueCapacity(long storageQueueCapacity) {
        this.storageQueueCapacity = storageQueueCapacity;
    }

    public HistoryStackVo getTps() {
        return tps;
    }

    public void setTps(HistoryStackVo tps) {
        this.tps = tps;
    }

    public HistoryStackVo getCpu() {
        return cpu;
    }

    public void setCpu(HistoryStackVo cpu) {
        this.cpu = cpu;
    }

    public HistoryStackVo getMemory() {
        return memory;
    }

    public void setMemory(HistoryStackVo memory) {
        this.memory = memory;
    }

    public List<MetricResponseVo> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<MetricResponseVo> metrics) {
        this.metrics = metrics;
    }
}
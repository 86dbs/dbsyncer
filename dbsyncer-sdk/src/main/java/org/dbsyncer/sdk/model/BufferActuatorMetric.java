/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

/**
 * 缓存执行器监控快照。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-12
 */
public class BufferActuatorMetric {

    /**
     * 驱动 Meta ID
     */
    private String metaId;

    /**
     * 展示名
     */
    private String name;

    /**
     * 缓存队列堆积数
     */
    private int queueSize;

    /**
     * 缓存队列容量
     */
    private int queueCapacity;

    /**
     * 写线程池活跃线程数
     */
    private int activeCount;

    /**
     * 写线程池最大线程数
     */
    private int maxPoolSize;

    /**
     * 写线程池已完成任务数
     */
    private long completedTaskCount;

    public String getMetaId() {
        return metaId;
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getActiveCount() {
        return activeCount;
    }

    public void setActiveCount(int activeCount) {
        this.activeCount = activeCount;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public long getCompletedTaskCount() {
        return completedTaskCount;
    }

    public void setCompletedTaskCount(long completedTaskCount) {
        this.completedTaskCount = completedTaskCount;
    }
}

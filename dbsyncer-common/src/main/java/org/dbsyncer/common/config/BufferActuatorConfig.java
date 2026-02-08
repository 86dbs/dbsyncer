package org.dbsyncer.common.config;

/**
 * 缓冲区配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/14 23:50
 */
public abstract class BufferActuatorConfig {

    /**
     * 单次执行任务数
     */
    private int bufferWriterCount = 100;

    /**
     * 每次消费缓存队列的任务数
     */
    private int bufferPullCount = 1000;

    /**
     * 缓存队列容量
     */
    private int bufferQueueCapacity = 3_0000;

    /**
     * 定时消费缓存队列间隔(毫秒)
     */
    private int bufferPeriodMillisecond = 300;

    public int getBufferWriterCount() {
        return bufferWriterCount;
    }

    public void setBufferWriterCount(int bufferWriterCount) {
        this.bufferWriterCount = bufferWriterCount;
    }

    public int getBufferPullCount() {
        return bufferPullCount;
    }

    public void setBufferPullCount(int bufferPullCount) {
        this.bufferPullCount = bufferPullCount;
    }

    public int getBufferQueueCapacity() {
        return bufferQueueCapacity;
    }

    public void setBufferQueueCapacity(int bufferQueueCapacity) {
        this.bufferQueueCapacity = bufferQueueCapacity;
    }

    public int getBufferPeriodMillisecond() {
        return bufferPeriodMillisecond;
    }

    public void setBufferPeriodMillisecond(int bufferPeriodMillisecond) {
        this.bufferPeriodMillisecond = bufferPeriodMillisecond;
    }
}

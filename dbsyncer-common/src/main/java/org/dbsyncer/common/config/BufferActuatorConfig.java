package org.dbsyncer.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 缓冲区配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/14 23:50
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.parser.buffer.actuator")
public class BufferActuatorConfig implements Cloneable {

    /**
     * 写批量数
     */
    private int writerBatchCount = 100;

    /**
     * 批量同步数
     */
    private int batchCount = 1000;

    /**
     * 工作线任务队列
     */
    private int queueCapacity = 5_0000;

    /**
     * 同步间隔（毫秒）
     */
    private int periodMillisecond = 300;

    public int getWriterBatchCount() {
        return writerBatchCount;
    }

    public void setWriterBatchCount(int writerBatchCount) {
        this.writerBatchCount = writerBatchCount;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getPeriodMillisecond() {
        return periodMillisecond;
    }

    public void setPeriodMillisecond(int periodMillisecond) {
        this.periodMillisecond = periodMillisecond;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
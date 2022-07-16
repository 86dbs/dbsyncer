package org.dbsyncer.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/14 23:50
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.storage.binlog.recorder")
public class BinlogRecorderConfig {

    /**
     * 批量同步数
     */
    private int batchCount = 1000;

    /**
     * 最长任务处理耗时（秒）
     */
    private int maxProcessingSeconds = 120;

    /**
     * 工作线任务队列
     */
    private int queueCapacity = 10000;

    /**
     * 写磁盘间隔（毫秒）
     */
    private int writerPeriodMillisecond = 500;

    /**
     * 读磁盘间隔（毫秒）
     */
    private int readerPeriodMillisecond = 2000;

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public int getMaxProcessingSeconds() {
        return maxProcessingSeconds;
    }

    public void setMaxProcessingSeconds(int maxProcessingSeconds) {
        this.maxProcessingSeconds = maxProcessingSeconds;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getWriterPeriodMillisecond() {
        return writerPeriodMillisecond;
    }

    public void setWriterPeriodMillisecond(int writerPeriodMillisecond) {
        this.writerPeriodMillisecond = writerPeriodMillisecond;
    }

    public int getReaderPeriodMillisecond() {
        return readerPeriodMillisecond;
    }

    public void setReaderPeriodMillisecond(int readerPeriodMillisecond) {
        this.readerPeriodMillisecond = readerPeriodMillisecond;
    }
}
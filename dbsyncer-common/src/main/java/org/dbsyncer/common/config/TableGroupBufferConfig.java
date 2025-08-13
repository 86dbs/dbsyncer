package org.dbsyncer.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 表执行器配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023/8/28 23:50
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.parser.table.group")
public class TableGroupBufferConfig extends BufferActuatorConfig {

    /**
     * 工作线程数
     */
    private int threadCoreSize = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * 最大工作线程数
     */
    private int maxThreadSize = 10;

    /**
     * 工作线任务队列
     */
    private int threadQueueCapacity = 1000;

    public int getThreadCoreSize() {
        return threadCoreSize;
    }

    public void setThreadCoreSize(int threadCoreSize) {
        this.threadCoreSize = threadCoreSize;
    }

    public int getMaxThreadSize() {
        return maxThreadSize;
    }

    public void setMaxThreadSize(int maxThreadSize) {
        this.maxThreadSize = maxThreadSize;
    }

    public int getThreadQueueCapacity() {
        return threadQueueCapacity;
    }

    public void setThreadQueueCapacity(int threadQueueCapacity) {
        this.threadQueueCapacity = threadQueueCapacity;
    }
}
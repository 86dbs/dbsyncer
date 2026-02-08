/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.common.config;

import org.dbsyncer.common.util.ThreadPoolUtil;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 持久化配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023/8/28 23:50
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.storage")
public class StorageConfig extends BufferActuatorConfig {

    /**
     * 工作线程数
     */
    private int threadCoreSize = Runtime.getRuntime().availableProcessors();

    /**
     * 最大工作线程数
     */
    private int maxThreadSize = threadCoreSize * 2;;

    /**
     * 工作线任务队列
     */
    private int threadQueueCapacity = 500;

    @Bean(name = "storageExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor storageExecutor() {
        return ThreadPoolUtil.newThreadPoolTaskExecutor(threadCoreSize, maxThreadSize, threadQueueCapacity, 30, "StorageExecutor-");
    }

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

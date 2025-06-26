/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.common.config;

import org.dbsyncer.common.util.ThreadPoolUtil;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 调度任务执行器配置
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-22 23:20
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.biz.dispatch")
public class DispatchTaskConfig {
    /**
     * 工作线程数
     */
    private int threadCoreSize = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * 最大工作线程数
     */
    private int maxThreadSize = 16;

    /**
     * 工作线任务队列
     */
    private int threadQueueCapacity = 64;

    @Bean(name = "dispatchTaskExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor dispatchTaskExecutor() {
        return ThreadPoolUtil.newThreadPoolTaskExecutor(threadCoreSize, maxThreadSize, threadQueueCapacity, 30, "DispatchTaskExecutor-");
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
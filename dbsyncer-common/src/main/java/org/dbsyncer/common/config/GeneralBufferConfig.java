package org.dbsyncer.common.config;

import org.dbsyncer.common.util.ThreadPoolUtil;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 通用执行器配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023/8/28 23:50
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.parser.general")
public class GeneralBufferConfig extends BufferActuatorConfig {
    /**
     * 工作线程数
     */
    private int threadCoreSize = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * 最大工作线程数
     */
    private int maxThreadSize = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * 工作线任务队列
     */
    private int threadQueueCapacity = 1000;

    @Bean(name = "generalExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor generalExecutor() {
        // 确保核心线程数不超过最大线程数
        int coreSize = Math.min(threadCoreSize, maxThreadSize);
        return ThreadPoolUtil.newThreadPoolTaskExecutor(coreSize, maxThreadSize, threadQueueCapacity, 30, "GeneralExecutor-");
    }

    public int getThreadCoreSize() {
        return threadCoreSize;
    }

    public void setThreadCoreSize(int threadCoreSize) {
        this.threadCoreSize = threadCoreSize;
    }

    public int getMaxThreadSize() {
        // 确保maxThreadSize不小于threadCoreSize
        return Math.max(maxThreadSize, threadCoreSize);
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
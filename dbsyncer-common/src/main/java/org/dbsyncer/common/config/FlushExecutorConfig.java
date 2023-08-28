package org.dbsyncer.common.config;

import org.dbsyncer.common.util.ThreadPoolUtil;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 持久化线程池配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020-04-26 23:40
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.parser.flush.executor")
public class FlushExecutorConfig {

    /**
     * 工作线程数
     */
    private int coreSize = Runtime.getRuntime().availableProcessors();

    /**
     * 工作线任务队列
     */
    private int queueCapacity = 300;

    @Bean(name = "flushExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor flushExecutor() {
        return ThreadPoolUtil.newThreadPoolTaskExecutor(coreSize, coreSize, queueCapacity, 30, "flushExecutor-");
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getCoreSize() {
        return coreSize;
    }

    public void setCoreSize(int coreSize) {
        this.coreSize = coreSize;
    }

}
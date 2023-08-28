package org.dbsyncer.common.config;

import org.dbsyncer.common.util.ThreadPoolUtil;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020-04-26 23:40
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.parser.write.executor")
public class WriteExecutorConfig {

    /**
     * 工作线程数
     */
    private int coreSize = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * 工作线任务队列
     */
    private int queueCapacity = 1000;

    @Bean(name = "writeExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor writeExecutor() {
        return ThreadPoolUtil.newThreadPoolTaskExecutor(coreSize, coreSize, queueCapacity, 30, "writeExecutor-");
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
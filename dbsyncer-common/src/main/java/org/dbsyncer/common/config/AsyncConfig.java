package org.dbsyncer.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2025/10/23 18:30
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    private final int poolSize = Runtime.getRuntime().availableProcessors();

    @Bean(name = "asyncExecutor")
    public Executor asyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setQueueCapacity(1024);
        executor.setThreadNamePrefix("async-");
        executor.initialize();
        return executor;
    }

}
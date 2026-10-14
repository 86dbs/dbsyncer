/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.manager;

import org.dbsyncer.manager.deployment.StandaloneFullFlushService;
import org.dbsyncer.manager.deployment.StandaloneService;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.FullFlushService;
import org.dbsyncer.sdk.spi.ServiceFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Resource;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023-11-19 23:29
 */
@Configuration
public class ManagerSupportConfiguration {

    @Resource
    private ServiceFactory serviceFactory;

    @Bean
    @ConditionalOnMissingBean(ClusterService.class)
    @DependsOn(value = "serviceFactory")
    public ClusterService clusterService() {
        ClusterService spi = serviceFactory.get(ClusterService.class);
        if (spi != null) {
            return spi;
        }
        return new StandaloneService();
    }

    /**
     * 全量页级落库：优先 SPI，否则单机默认实现。
     */
    @Bean
    @ConditionalOnMissingBean(FullFlushService.class)
    @DependsOn(value = "serviceFactory")
    public FullFlushService fullFlushService() {
        FullFlushService spi = serviceFactory.get(FullFlushService.class);
        if (spi != null) {
            return spi;
        }
        return new StandaloneFullFlushService();
    }

}

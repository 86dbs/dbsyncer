/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.manager;

import org.dbsyncer.manager.deployment.StandaloneProvider;
import org.dbsyncer.sdk.spi.DeploymentService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ServiceLoader;

/**
 * @author AE86
 * @version 1.0.0
 * @Date 2023-11-19 23:29
 */
@Configuration
public class ManagerSupportConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DeploymentService deploymentService() {
        ServiceLoader<DeploymentService> services = ServiceLoader.load(DeploymentService.class, Thread.currentThread().getContextClassLoader());
        for (DeploymentService s : services) {
            return s;
        }
        return new StandaloneProvider();
    }

}
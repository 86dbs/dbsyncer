/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.manager;

import org.dbsyncer.manager.deployment.StandaloneProvider;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.DeploymentService;
import org.dbsyncer.sdk.spi.LicenseService;
import org.dbsyncer.sdk.spi.ServiceFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

import javax.annotation.Resource;

/**
 * 部署装配：商业集群 SPI 优先，缺省单机。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023-11-19 23:29
 */
@Configuration
public class ManagerSupportConfiguration {

    @Resource
    private ServiceFactory serviceFactory;

    @Bean
    @ConditionalOnMissingBean(DeploymentService.class)
    @DependsOn(value = "serviceFactory")
    public DeploymentService deploymentService(MetaProfile metaProfile, LicenseService licenseService, Environment environment) {
        DeploymentService commercial = serviceFactory.get(DeploymentService.class);
        boolean clusterEnabled = environment.getProperty("dbsyncer.cluster.enabled", Boolean.class, Boolean.FALSE);
        String storageType = environment.getProperty("dbsyncer.storage.type", "h2");
        if (commercial != null && commercial.isClusterRuntime(licenseService, clusterEnabled, storageType)) {
            return commercial;
        }
        return new StandaloneProvider(metaProfile);
    }

    @Bean
    @ConditionalOnMissingBean(ClusterService.class)
    public ClusterService clusterService(DeploymentService deploymentService) {
        return deploymentService.getClusterService();
    }
}

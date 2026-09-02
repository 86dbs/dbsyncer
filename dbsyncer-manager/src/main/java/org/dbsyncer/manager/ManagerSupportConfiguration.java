/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.manager;

import org.dbsyncer.manager.deployment.StandaloneService;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.LicenseService;
import org.dbsyncer.sdk.spi.ServiceFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

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
    public ClusterService clusterService(MetaProfile metaProfile, LicenseService licenseService, Environment environment) {
        ClusterService spi = serviceFactory.get(ClusterService.class);
        boolean clusterEnabled = environment.getProperty("dbsyncer.cluster.enabled", Boolean.class, Boolean.FALSE);
        String storageType = environment.getProperty("dbsyncer.storage.type", "h2");
        if (spi != null && spi.isClusterRuntime(licenseService, clusterEnabled, storageType)) {
            return spi;
        }
        return new StandaloneService(metaProfile);
    }
}

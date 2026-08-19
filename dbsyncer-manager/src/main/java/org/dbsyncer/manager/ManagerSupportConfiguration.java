/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.manager;

import org.dbsyncer.manager.deployment.StandaloneProvider;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.DeploymentService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 部署装配：缺省单机。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023-11-19 23:29
 */
@Configuration
public class ManagerSupportConfiguration {

    @Bean
    @ConditionalOnMissingBean(DeploymentService.class)
    public DeploymentService deploymentService(MetaProfile metaProfile) {
        return new StandaloneProvider(metaProfile);
    }

    @Bean
    @ConditionalOnMissingBean(ClusterService.class)
    public ClusterService clusterService(DeploymentService deploymentService) {
        return deploymentService.getClusterService();
    }
}

/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.manager;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.deployment.StandaloneService;
import org.dbsyncer.sdk.spi.ClusterService;
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
    public ClusterService clusterService(Environment environment) {
        ClusterService spi = serviceFactory.get(ClusterService.class);
        boolean clusterEnabled = environment.getProperty("dbsyncer.cluster.enabled", Boolean.class, Boolean.FALSE);
        String storageType = environment.getProperty("dbsyncer.storage.type");
        // 暂仅支持MySQL
        if (spi != null && clusterEnabled && StringUtil.equalsIgnoreCase("MySQL", storageType)) {
            return spi;
        }
        return new StandaloneService();
    }
}

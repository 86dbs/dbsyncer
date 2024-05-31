/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.impl.DefaultLicenseServiceImpl;
import org.dbsyncer.sdk.spi.LicenseService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ServiceLoader;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-14 01:30
 */
@Configuration
public class BizSupportConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public LicenseService licenseService() {
        ServiceLoader<LicenseService> services = ServiceLoader.load(LicenseService.class, Thread.currentThread().getContextClassLoader());
        for (LicenseService s : services) {
            return s;
        }
        return new DefaultLicenseServiceImpl();
    }
}
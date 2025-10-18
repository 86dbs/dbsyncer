/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.ProductInfo;
import org.dbsyncer.sdk.spi.LicenseService;
import org.dbsyncer.sdk.spi.ServiceFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.io.File;
import java.util.ServiceLoader;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-07-05 00:30
 */
@Configuration
public class SdkSupportConfiguration {



    @Bean
    @ConditionalOnMissingBean
    public LicenseService licenseService() {
        ServiceLoader<LicenseService> services = ServiceLoader.load(LicenseService.class, Thread.currentThread().getContextClassLoader());
        for (LicenseService s : services) {
            return s;
        }
        return new LicenseService() {
            @Override
            public String getLicensePath() {
                return System.getProperty("user.dir") + File.separatorChar + "conf" + File.separatorChar;
            }

            @Override
            public String getKey() {
                return StringUtil.EMPTY;
            }

            @Override
            public ProductInfo getProductInfo() {
                return null;
            }

            @Override
            public void updateLicense() {

            }
        };
    }

    @Bean
    @ConditionalOnMissingBean
    @DependsOn(value = "licenseService")
    public ServiceFactory serviceFactory() {
        ServiceLoader<ServiceFactory> services = ServiceLoader.load(ServiceFactory.class, Thread.currentThread().getContextClassLoader());
        for (ServiceFactory s : services) {
            return s;
        }
        return new ServiceFactory() {
            @Override
            public <T> T get(Class<T> serviceClass) {
                return null;
            }
        };
    }


}
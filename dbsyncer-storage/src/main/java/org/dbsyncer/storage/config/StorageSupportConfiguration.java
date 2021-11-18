package org.dbsyncer.storage.config;

import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.support.DiskStorageServiceImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 21:36
 */
@Configuration
public class StorageSupportConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public StorageService storageService() {
        return new DiskStorageServiceImpl();
    }

}
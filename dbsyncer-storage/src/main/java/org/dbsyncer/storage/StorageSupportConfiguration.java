/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.DiskStorageService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.origin.OriginTrackedValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 21:36
 */
@Configuration
public class StorageSupportConfiguration {

    private final String PREFIX_STORAGE = "dbsyncer.storage";

    private final String STORAGE_TYPE = "dbsyncer.storage.type";

    @Resource
    private Environment environment;

    @Resource
    private ConnectorFactory connectorFactory;

    @Bean
    @ConditionalOnMissingBean
    public StorageService storageService() {
        Properties properties = new Properties();
        if (environment instanceof AbstractEnvironment) {
            AbstractEnvironment ae = (AbstractEnvironment) environment;
            MutablePropertySources propertySources = ae.getPropertySources();
            for (PropertySource<?> propertySource : propertySources) {
                boolean applicationConfig = propertySource.getName().contains("application");
                if (!applicationConfig) {
                    continue;
                }
                Map<String, OriginTrackedValue> props = (Map<String, OriginTrackedValue>) propertySource.getSource();
                props.forEach((k, v) -> {
                    if (StringUtil.startsWith(k, PREFIX_STORAGE)) {
                        properties.put(k, v.getValue());
                    }
                });
            }
        }

        // 指定存储类型
        String storageType = properties.getProperty(STORAGE_TYPE);
        if (StringUtil.isNotBlank(storageType)) {
            String connectorType = getConnectorType(storageType);
            if (StringUtil.isNotBlank(connectorType)) {
                StorageService storageService = connectorFactory.getConnectorService(connectorType).getStorageService();
                if (storageService != null) {
                    storageService.init(properties);
                    return storageService;
                }
            }
        }

        // 默认磁盘存储
        DiskStorageService storageService = new DiskStorageService();
        storageService.init(properties);
        return storageService;
    }

    private String getConnectorType(String storageType) {
        Iterator<String> iterator = connectorFactory.getConnectorTypeAll().iterator();
        while (iterator.hasNext()) {
            String connectorType = iterator.next();
            if (StringUtil.equalsIgnoreCase(storageType, connectorType)) {
                return connectorType;
            }
        }
        return null;
    }

}
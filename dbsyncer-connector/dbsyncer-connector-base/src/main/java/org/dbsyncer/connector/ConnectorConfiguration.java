/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector;

import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.spi.StorageService;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.beans.Introspector;
import java.util.ServiceLoader;

/**
 * 连接器配置
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-09-19 23:17
 */
@Service
public class ConnectorConfiguration implements BeanDefinitionRegistryPostProcessor {

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry beanDefinitionRegistry) throws BeansException {
        loadConnectorServices(beanDefinitionRegistry);
//        loadStorageServices(beanDefinitionRegistry);
    }

    private void loadStorageServices(BeanDefinitionRegistry beanDefinitionRegistry) {
        ServiceLoader<StorageService> services = ServiceLoader.load(StorageService.class, Thread.currentThread().getContextClassLoader());
        for (StorageService s : services) {
            BeanDefinition beanDefinition = BeanDefinitionBuilder.genericBeanDefinition(s.getClass()).getBeanDefinition();
            String beanClassName = beanDefinition.getBeanClassName();
            Assert.state(beanClassName != null, "No bean class name set");
            String shortClassName = ClassUtils.getShortName(beanClassName);
            String decapitalize = Introspector.decapitalize(shortClassName);
            beanDefinitionRegistry.registerBeanDefinition(decapitalize, beanDefinition);
        }
    }

    private void loadConnectorServices(BeanDefinitionRegistry beanDefinitionRegistry) {
        ServiceLoader<ConnectorService> services = ServiceLoader.load(ConnectorService.class, Thread.currentThread().getContextClassLoader());
        for (ConnectorService s : services) {
            BeanDefinition beanDefinition = BeanDefinitionBuilder.genericBeanDefinition(s.getClass()).getBeanDefinition();
            String beanClassName = beanDefinition.getBeanClassName();
            Assert.state(beanClassName != null, "No bean class name set");
            String shortClassName = ClassUtils.getShortName(beanClassName);
            String decapitalize = Introspector.decapitalize(shortClassName);
            beanDefinitionRegistry.registerBeanDefinition(decapitalize, beanDefinition);
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {

    }
}

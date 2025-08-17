/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.sdk.spi.ServiceFactory;
import org.dbsyncer.sdk.spi.TableGroupBufferActuatorService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Resource;
import java.util.ServiceLoader;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-01-25 23:43
 */
@Configuration
public class ParserSupportConfiguration {

    @Resource
    private ServiceFactory serviceFactory;

    @Bean
    @ConditionalOnMissingBean
    @DependsOn(value = "serviceFactory")
    public TableGroupBufferActuatorService tableGroupBufferActuatorService() {
        ServiceLoader<TableGroupBufferActuatorService> services = ServiceLoader.load(TableGroupBufferActuatorService.class, Thread.currentThread().getContextClassLoader());
        for (TableGroupBufferActuatorService s : services) {
            return s;
        }
        return new TableGroupBufferActuator();
    }
}
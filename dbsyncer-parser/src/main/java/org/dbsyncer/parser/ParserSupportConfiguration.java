/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.sdk.spi.TableGroupBufferActuatorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ServiceLoader;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-01-25 23:43
 */
@Configuration
public class ParserSupportConfiguration {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Bean
    @ConditionalOnMissingBean
    public TableGroupBufferActuator tableGroupBufferActuator() {
        ServiceLoader<TableGroupBufferActuatorService> services = ServiceLoader.load(TableGroupBufferActuatorService.class, Thread.currentThread().getContextClassLoader());
        for (TableGroupBufferActuatorService s : services) {
            try {
                TableGroupBufferActuatorService service = s.getClass().newInstance();
                if (service instanceof TableGroupBufferActuator) {
                    return (TableGroupBufferActuator) service;
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new ParserException("获取TableGroupBufferActuator异常.");
            }
        }
        return new TableGroupBufferActuator();
    }
}
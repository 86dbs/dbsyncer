/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.parser.flush.impl.DefaultBufferActuatorRouter;
import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.sdk.spi.BufferActuatorRouterService;
import org.dbsyncer.sdk.spi.DatabaseSyncDetailService;
import org.dbsyncer.sdk.spi.ServiceFactory;
import org.dbsyncer.sdk.spi.TableGroupBufferActuatorService;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.sdk.spi.ValidateSyncDetailService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Resource;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2024-01-25 23:43
 */
@Configuration
public class ParserSupportConfiguration {

    @Resource
    private ServiceFactory serviceFactory;

    @Bean
    @ConditionalOnMissingBean
    @DependsOn(value = "serviceFactory")
    public TableGroupBufferActuatorService tableGroupBufferActuatorService() {
        TableGroupBufferActuatorService service = serviceFactory.get(TableGroupBufferActuatorService.class);
        if (service != null) {
            return service;
        }
        return new TableGroupBufferActuator();
    }

    @Bean
    @ConditionalOnMissingBean
    @DependsOn(value = "serviceFactory")
    public BufferActuatorRouterService bufferActuatorRouterService() {
        BufferActuatorRouterService service = serviceFactory.get(BufferActuatorRouterService.class);
        if (service != null) {
            return service;
        }
        return new DefaultBufferActuatorRouter();
    }

    @Bean
    @ConditionalOnMissingBean
    @DependsOn(value = "serviceFactory")
    public TaskService taskService() {
        TaskService taskService = serviceFactory.get(TaskService.class);
        if (taskService != null) {
            return taskService;
        }
        return new TaskService<ConfigModel>() {
        };
    }

    @Bean
    @ConditionalOnMissingBean
    @DependsOn(value = "serviceFactory")
    public DatabaseSyncDetailService dataBaseSyncerDetailService() {
        DatabaseSyncDetailService service = serviceFactory.get(DatabaseSyncDetailService.class);
        if (service != null) {
            return service;
        }
        return new DatabaseSyncDetailService() {
        };
    }

    @Bean
    @ConditionalOnMissingBean
    @DependsOn(value = {"serviceFactory", "taskService"})
    public ValidateSyncDetailService validateSyncerDetailService() {
        ValidateSyncDetailService service = serviceFactory.get(ValidateSyncDetailService.class);
        if (service != null) {
            return service;
        }
        return new ValidateSyncDetailService() {};
    }
}
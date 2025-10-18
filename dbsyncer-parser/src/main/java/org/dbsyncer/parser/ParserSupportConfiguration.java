/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.sdk.model.CommonTask;
import org.dbsyncer.sdk.spi.ServiceFactory;
import org.dbsyncer.sdk.spi.TableGroupBufferActuatorService;
import org.dbsyncer.sdk.spi.TaskService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Resource;
import java.util.Map;

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
        TableGroupBufferActuatorService service = serviceFactory.get(TableGroupBufferActuatorService.class);
        if (service != null) {
            return service;
        }
        return new TableGroupBufferActuator();
    }

    @Bean
    @ConditionalOnMissingBean
    @DependsOn(value = "serviceFactory")
    public TaskService taskService() {
        TaskService taskService = serviceFactory.get(TaskService.class);
        if (taskService != null) {
            return taskService;
        }
        return new TaskService() {
            @Override
            public void add(Map<String, String> params) {

            }

            @Override
            public void modify(Map<String, String> params) {

            }

            @Override
            public void delete(String taskId) {

            }

            @Override
            public void start(String taskId) {

            }

            @Override
            public void stop(String taskId) {

            }

            @Override
            public CommonTask detail(String taskId) {
                return null;
            }

            @Override
            public Paging list(Map<String, String> param) {
                return null;
            }

            @Override
            public Paging result(Map<String, String> param) {
                return null;
            }
        };
    }


}
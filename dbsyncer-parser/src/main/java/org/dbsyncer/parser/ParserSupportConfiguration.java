/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.StringUtil;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    public TaskService taskService() {
        TaskService taskService = serviceFactory.get(TaskService.class);
        if (taskService != null) {
            return taskService;
        }
        return new TaskService<CommonTask>() {

            @Override
            public String add(CommonTask task) {
                return StringUtil.EMPTY;
            }

            @Override
            public String edit(CommonTask task) {
                return StringUtil.EMPTY;
            }

            @Override
            public void delete(String id) {

            }

            @Override
            public void start(String id) {

            }

            @Override
            public void stop(String id) {

            }

            @Override
            public CommonTask get(String id) {
                return null;
            }

            @Override
            public Paging search(Map<String, String> param, CommonTaskTypeEnum commonTaskTypeEnum) {
                return null;
            }

            @Override
            public Paging result(String id) {
                return null;
            }

            @Override
            public List<CommonTask> getTaskAll(CommonTaskTypeEnum commonTaskTypeEnum) {
                return Collections.emptyList();
            }

            @Override
            public boolean isRunning(String taskId) {
                return false;
            }

        };
    }

}
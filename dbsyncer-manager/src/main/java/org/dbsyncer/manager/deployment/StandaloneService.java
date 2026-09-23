/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.deployment;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.event.FullRefreshEvent;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Task;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.service.TaskManager;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.TaskService;
import org.springframework.context.ApplicationContext;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 单机控制面：本机即执行者，调度方法空操作。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class StandaloneService implements ClusterService {

    @Resource
    private TaskManager taskManager;

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private TaskService taskService;

    @Override
    public void start(ConfigModel configModel, boolean autoRecovery) {
        if (configModel instanceof Mapping) {
            taskManager.start(configModel, autoRecovery);
        } else {
            //todo 转换为 model
            taskService.start(configModel);
        }
    }

    @Override
    public void stop(String taskId) {
        taskManager.stop(taskId);
    }

    @Override
    public void flush(Task task, Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap) {
        result.setMetaId(task.getId());
        result.setEvent(ConnectorConstant.OPERTION_INSERT);
        flushStrategy.flushFullData(result, targetSchemaResolver, targetFieldMap);
        if (!task.isSkipTableProgressEvent()) {
            applicationContext.publishEvent(new FullRefreshEvent(applicationContext, task));
        }
    }

}

package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.StorageRequest;
import org.dbsyncer.parser.model.StorageResponse;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.enums.StorageEnum;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * 持久化任务缓冲执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public final class StorageBufferActuator extends AbstractBufferActuator<StorageRequest, StorageResponse> implements ScheduledTaskJob {

    @Resource
    private BufferActuatorConfig bufferActuatorConfig;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private StorageService storageService;

    @PostConstruct
    private void init() {
        super.buildConfig(bufferActuatorConfig);
        scheduledTaskService.start(bufferActuatorConfig.getPeriodMillisecond(), this);
    }

    @Override
    protected String getPartitionKey(StorageRequest request) {
        return request.getMetaId();
    }

    @Override
    protected void partition(StorageRequest request, StorageResponse response) {
        response.setMetaId(request.getMetaId());
        response.getDataList().add(request.getRow());
    }

    @Override
    protected void pull(StorageResponse response) {
        storageService.addBatch(StorageEnum.DATA, response.getMetaId(), response.getDataList());
    }

    @Override
    public void run() {
        batchExecute();
    }

}
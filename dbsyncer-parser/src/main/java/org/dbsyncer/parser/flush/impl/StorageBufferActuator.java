package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.flush.BufferResponse;
import org.dbsyncer.parser.flush.model.StorageRequest;
import org.dbsyncer.parser.flush.model.StorageResponse;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.enums.StorageEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public class StorageBufferActuator extends AbstractBufferActuator<StorageRequest, StorageResponse> {

    @Autowired
    private StorageService storageService;

    @Override
    protected long getPeriod() {
        return 500;
    }

    @Override
    protected BufferResponse getValue() {
        return new StorageResponse();
    }

    @Override
    protected String getPartitionKey(StorageRequest bufferTask) {
        return bufferTask.getMetaId();
    }

    @Override
    protected void partition(StorageRequest request, StorageResponse response) {
        response.setMetaId(request.getMetaId());
        response.getDataList().addAll(request.getList());
    }

    @Override
    protected void pull(StorageResponse response) {
        storageService.addData(StorageEnum.DATA, response.getMetaId(), response.getDataList());
    }
}
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.StorageRequest;
import org.dbsyncer.parser.model.StorageResponse;
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
    public int getQueueCapacity() {
        return 1_0000;
    }

    @Override
    protected String getPartitionKey(StorageRequest bufferTask) {
        return bufferTask.getMetaId();
    }

    @Override
    protected void partition(StorageRequest request, StorageResponse response) {
        response.setMetaId(request.getMetaId());
        response.getDataList().add(request.getRow());
    }

    @Override
    protected void pull(StorageResponse response) {
        storageService.addData(StorageEnum.DATA, response.getMetaId(), response.getDataList());
    }
}
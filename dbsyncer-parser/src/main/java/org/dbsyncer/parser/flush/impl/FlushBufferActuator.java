package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.flush.model.AbstractResponse;
import org.dbsyncer.parser.flush.model.FlushRequest;
import org.dbsyncer.parser.flush.model.FlushResponse;
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
public class FlushBufferActuator extends AbstractBufferActuator<FlushRequest, FlushResponse> {

    @Autowired
    private StorageService storageService;

    @Override
    protected long getPeriod() {
        return 3000;
    }

    @Override
    protected AbstractResponse getValue() {
        return new FlushResponse();
    }

    @Override
    protected String getPartitionKey(FlushRequest bufferTask) {
        return bufferTask.getMetaId();
    }

    @Override
    protected void partition(FlushRequest request, FlushResponse response) {
        response.setMetaId(request.getMetaId());
        response.getDataList().addAll(request.getList());
    }

    @Override
    protected void flush(FlushResponse response) {
        storageService.addData(StorageEnum.DATA, response.getMetaId(), response.getDataList());
    }
}
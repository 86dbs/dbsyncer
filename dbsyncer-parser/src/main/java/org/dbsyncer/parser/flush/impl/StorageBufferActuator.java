package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.StorageConfig;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.StorageRequest;
import org.dbsyncer.parser.model.StorageResponse;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.enums.StorageEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * 持久化执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public final class StorageBufferActuator extends AbstractBufferActuator<StorageRequest, StorageResponse> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private StorageConfig storageConfig;

    @Resource
    private StorageService storageService;

    @Resource
    private Executor storageExecutor;

    @Resource
    private ProfileComponent profileComponent;

    @PostConstruct
    private void init() {
        setConfig(storageConfig);
        buildConfig();
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
        storageExecutor.execute(() -> storageService.addBatch(StorageEnum.DATA, response.getMetaId(), response.getDataList()));
    }

    @Override
    protected void offerFailed(BlockingQueue<StorageRequest> queue, StorageRequest request) {
        Meta meta = profileComponent.getMeta(request.getMetaId());
        if (meta != null) {
            Mapping mapping = profileComponent.getMapping(meta.getMappingId());
            if (mapping != null) {
                logger.info("{}, data={}", mapping.getName(), request.getRow());
            }
        }
    }

    @Override
    public Executor getExecutor() {
        return storageExecutor;
    }
}
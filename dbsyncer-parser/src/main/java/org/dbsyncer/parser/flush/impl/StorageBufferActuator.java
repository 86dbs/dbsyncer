/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.StorageConfig;
import org.dbsyncer.parser.MappingProfile;
import org.dbsyncer.parser.TaskMetaProfile;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.StorageRequest;
import org.dbsyncer.parser.model.StorageResponse;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * 持久化执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023-03-27 16:50
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
    private MappingProfile mappingProfile;

    @Resource
    private TaskMetaProfile taskMetaProfile;

    @PostConstruct
    private void init() {
        setConfig(storageConfig);
        buildConfig();
    }

    @Override
    protected String getPartitionKey(StorageRequest request) {
        return request.getTaskDetailShardId();
    }

    @Override
    protected void partition(StorageRequest request, StorageResponse response) {
        response.setTaskDetailShardId(request.getTaskDetailShardId());
        response.getDataList().add(request.getRow());
    }

    @Override
    public void pull(StorageResponse response) {
        String shardId = response.getTaskDetailShardId();
        // 必须拷贝：process() finally 会 clearDataList，异步写不能共用同一 LinkedList（并发 clear 会把节点 item 置 null → NPE）
        List<Map> batch = new ArrayList<>(response.getDataList());
        storageExecutor.execute(() -> storageService.addBatch(StorageEnum.TASK_DETAIL, shardId, batch));
    }

    @Override
    protected void offerFailed(BlockingQueue<StorageRequest> queue, StorageRequest request) {
        String shardId = request.getTaskDetailShardId();
        Meta meta = taskMetaProfile.getMeta(shardId);
        if (meta != null) {
            Mapping mapping = mappingProfile.get(meta.getTaskId());
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
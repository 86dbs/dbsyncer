package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.StorageConfig;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.StorageRequest;
import org.dbsyncer.parser.model.StorageResponse;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.enums.StorageEnum;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 持久化执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public final class StorageBufferActuator extends AbstractBufferActuator<StorageRequest, StorageResponse> {

    private final Duration offerInterval = Duration.of(500, ChronoUnit.MILLIS);
    private final Lock queueLock = new ReentrantLock(true);
    private final Condition queueCondition = queueLock.newCondition();

    @Resource
    private StorageConfig storageConfig;

    @Resource
    private StorageService storageService;

    @Resource
    private Executor storageExecutor;

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
        storageService.addBatch(StorageEnum.DATA, response.getMetaId(), response.getDataList());
    }

    @Override
    protected void offerFailed(BlockingQueue<StorageRequest> queue, StorageRequest request) {
        final Lock lock = queueLock;
        try {
            // 公平锁，有序执行，容量上限，阻塞重试
            lock.lock();
            while (isRunning(request) && !queue.offer(request)) {
                try {
                    queueCondition.await(offerInterval.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Executor getExecutor() {
        return storageExecutor;
    }
}
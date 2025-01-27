/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.flush;

import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.metric.TimeRegistry;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.lang.reflect.ParameterizedType;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 任务缓存执行器
 * <p>1. 任务优先进入缓存队列
 * <p>2. 将任务分区合并，批量执行
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:36
 */
public abstract class AbstractBufferActuator<Request extends BufferRequest, Response extends BufferResponse> implements BufferActuator, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Class<Response> responseClazz;
    private Lock taskLock;
    private BufferActuatorConfig config;
    private BlockingQueue<Request> queue;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private TimeRegistry timeRegistry;

    public AbstractBufferActuator() {
        int level = 5;
        Class<?> aClass = getClass();
        while (level > 0) {
            if (aClass.getSuperclass() == AbstractBufferActuator.class) {
                responseClazz = (Class<Response>) ((ParameterizedType) aClass.getGenericSuperclass()).getActualTypeArguments()[1];
                break;
            }
            aClass = aClass.getSuperclass();
            level--;
        }
        Assert.notNull(responseClazz, String.format("%s的父类%s泛型参数Response为空.", getClass().getName(), AbstractBufferActuator.class.getName()));
    }

    /**
     * 初始化配置
     */
    protected void buildConfig() {
        Assert.notNull(config, "请先配置缓存执行器，setConfig(BufferActuatorConfig config)");
        buildQueueConfig();
        scheduledTaskService.start(config.getBufferPeriodMillisecond(), this);
    }

    /**
     * 初始化缓存队列配置
     */
    protected void buildQueueConfig() {
        taskLock = new ReentrantLock();
        this.queue = new LinkedBlockingQueue<>(config.getBufferQueueCapacity());
        logger.info("{} initialized with queue capacity: {}", this.getClass().getSimpleName(), config.getBufferQueueCapacity());
    }

    /**
     * 生成分区key
     *
     * @param request
     * @return
     */
    protected abstract String getPartitionKey(Request request);

    /**
     * 分区
     *
     * @param request
     * @param response
     */
    protected abstract void partition(Request request, Response response);

    /**
     * 驱动是否运行中
     *
     * @param request
     * @return
     */
    public boolean isRunning(BufferRequest request) {
        Meta meta = profileComponent.getMeta(request.getMetaId());
        return meta != null && MetaEnum.isRunning(meta.getState());
    }

    /**
     * 是否跳过分区处理
     *
     * @param nextRequest
     * @param response
     * @return
     */
    protected boolean skipPartition(Request nextRequest, Response response) {
        return false;
    }

    /**
     * 批处理
     *
     * @param response
     */
    public abstract void pull(Response response);

    /**
     * 批量处理分区数据
     *
     * @param map
     */
    protected void process(Map<String, Response> map) {
        map.forEach((key, response) -> {
            long now = Instant.now().toEpochMilli();
            try {
                pull(response);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("[{}{}]{}, {}ms", key, response.getSuffixName(), response.getTaskSize(), (Instant.now().toEpochMilli() - now));
        });
    }

    /**
     * 提交任务失败
     *
     * @param queue
     * @param request
     */
    protected abstract void offerFailed(BlockingQueue<Request> queue, Request request);

    /**
     * 统计消费 TPS
     *
     * @param timeRegistry
     * @param count
     */
    protected void meter(TimeRegistry timeRegistry, long count) {

    }

    @Override
    public void offer(BufferRequest request) {
        if (queue.offer((Request) request)) {
            return;
        }
        if (isRunning(request)) {
            offerFailed(queue, (Request) request);
        }
    }

    @Override
    public void run() {
        boolean locked = false;
        try {
            locked = taskLock.tryLock();
            if (locked) {
                submit();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (locked) {
                taskLock.unlock();
            }
        }
    }

    @Override
    public Queue getQueue() {
        return queue;
    }

    @Override
    public int getQueueCapacity() {
        return config.getBufferQueueCapacity();
    }

    private void submit() {
        if (queue.isEmpty()) {
            return;
        }

        AtomicLong batchCounter = new AtomicLong();
        Map<String, Response> map = new ConcurrentHashMap<>();
        while (!queue.isEmpty() && batchCounter.get() < config.getBufferPullCount()) {
            Request poll = queue.poll();
            String key = getPartitionKey(poll);
            Response response = map.compute(key, (k,v) -> {
                if (v == null) {
                    try {
                        return responseClazz.newInstance();
                    } catch (Exception e) {
                        throw new ParserException(e);
                    }
                }
                return v;
            });
            partition(poll, response);
            batchCounter.incrementAndGet();

            Request next = queue.peek();
            if (null != next && skipPartition(next, response)) {
                break;
            }
        }

        process(map);
        map.clear();
        meter(timeRegistry, batchCounter.get());
        map = null;
        batchCounter = null;
    }

    public void setConfig(BufferActuatorConfig config) {
        this.config = config;
    }

}
package org.dbsyncer.parser.flush;

import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.parser.CacheService;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
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
    private Lock queueLock;
    private Condition isFull;
    private final Duration offerInterval = Duration.of(500, ChronoUnit.MILLIS);
    private BufferActuatorConfig config;
    private BlockingQueue<Request> queue;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private CacheService cacheService;

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
        buildLock();
        buildQueueConfig();
        scheduledTaskService.start(config.getBufferPeriodMillisecond(), this);
    }

    /**
     * 初始化缓存队列配置
     */
    protected void buildQueueConfig() {
        this.queue = new LinkedBlockingQueue(config.getBufferQueueCapacity());
        logger.info("{} initialized with queue capacity: {}", this.getClass().getSimpleName(), config.getBufferQueueCapacity());
    }

    /**
     * 初始化锁
     */
    protected void buildLock(){
        taskLock = new ReentrantLock();
        queueLock = new ReentrantLock(true);
        isFull = queueLock.newCondition();
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
    protected boolean isRunning(BufferRequest request) {
        Meta meta = cacheService.get(request.getMetaId(), Meta.class);
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
    protected abstract void pull(Response response);

    @Override
    public void offer(BufferRequest request) {
        if (!queue.offer((Request) request)) {
            try {
                // 公平锁，有序执行，容量上限，阻塞重试
                queueLock.lock();
                while (isRunning(request) && !queue.offer((Request) request)) {
                    try {
                        isFull.await(offerInterval.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } finally {
                queueLock.unlock();
            }
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

    private void submit() throws IllegalAccessException, InstantiationException {
        if (queue.isEmpty()) {
            return;
        }

        AtomicLong batchCounter = new AtomicLong();
        Map<String, Response> map = new LinkedHashMap<>();
        while (!queue.isEmpty() && batchCounter.get() < config.getBufferPullCount()) {
            Request poll = queue.poll();
            String key = getPartitionKey(poll);
            if (!map.containsKey(key)) {
                map.putIfAbsent(key, responseClazz.newInstance());
            }
            Response response = map.get(key);
            partition(poll, response);
            batchCounter.incrementAndGet();

            Request next = queue.peek();
            if (null != next && skipPartition(next, response)) {
                break;
            }
        }

        map.forEach((key, response) -> {
            long now = Instant.now().toEpochMilli();
            try {
                pull(response);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("[{}{}]{}, {}ms", key, response.getSuffixName(), response.getTaskSize(), (Instant.now().toEpochMilli() - now));
        });
        map.clear();
        map = null;
        batchCounter = null;
    }

    public void setConfig(BufferActuatorConfig config) {
        this.config = config;
    }
}
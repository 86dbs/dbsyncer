package org.dbsyncer.parser.flush;

import org.dbsyncer.common.config.BufferActuatorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

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
public abstract class AbstractBufferActuator<Request extends BufferRequest, Response extends BufferResponse> implements BufferActuator {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Class<Response> responseClazz;
    private final Lock TASK_LOCK = new ReentrantLock(true);
    private volatile boolean running;
    private final Lock BUFFER_LOCK = new ReentrantLock();
    private final Condition IS_FULL = BUFFER_LOCK.newCondition();
    private final Duration OFFER_INTERVAL = Duration.of(500, ChronoUnit.MILLIS);
    private BufferActuatorConfig config;
    private BlockingQueue<Request> queue;

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
     *
     * @param config
     */
    public void buildConfig(BufferActuatorConfig config) {
        this.config = config;
        this.queue = new LinkedBlockingQueue(config.getQueueCapacity());
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
        boolean lock = false;
        try {
            lock = BUFFER_LOCK.tryLock();
            if (lock) {
                if (!queue.offer((Request) request)) {
                    logger.warn("[{}]缓存队列容量已达上限[{}], 正在阻塞重试.", this.getClass().getSimpleName(), getQueueCapacity());

                    // 容量上限，阻塞重试
                    while (!queue.offer((Request) request)) {
                        try {
                            IS_FULL.await(OFFER_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            }
        } finally {
            if (lock) {
                BUFFER_LOCK.unlock();
            }
        }
    }

    @Override
    public void batchExecute() {
        if (running) {
            return;
        }

        boolean locked = false;
        try {
            locked = TASK_LOCK.tryLock();
            if (locked) {
                running = true;
                submit();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (locked) {
                running = false;
                TASK_LOCK.unlock();
            }
        }
    }

    @Override
    public Queue getQueue() {
        return queue;
    }

    @Override
    public int getQueueCapacity() {
        return config.getQueueCapacity();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    private void submit() throws IllegalAccessException, InstantiationException {
        if (queue.isEmpty()) {
            return;
        }

        AtomicLong batchCounter = new AtomicLong();
        Map<String, Response> map = new LinkedHashMap<>();
        while (!queue.isEmpty() && batchCounter.get() < config.getBatchCount()) {
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
            final BufferResponse resp = (BufferResponse) response;
            logger.info("[{}{}]{}, {}ms", key, resp.getSuffixName(), resp.getTaskSize(), (Instant.now().toEpochMilli() - now));
        });
        map.clear();
        map = null;
        batchCounter = null;
    }

    public BufferActuatorConfig getConfig() {
        return config;
    }
}
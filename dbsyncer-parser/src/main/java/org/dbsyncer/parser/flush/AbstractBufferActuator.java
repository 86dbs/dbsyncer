package org.dbsyncer.parser.flush;

import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.lang.reflect.ParameterizedType;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
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
public abstract class AbstractBufferActuator<Request, Response> implements BufferActuator, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    @Autowired
    private BufferActuatorConfig bufferActuatorConfig;

    private LocalDateTime lastBufferWarningTime;

    private BlockingQueue<Request> buffer;

    private final Lock lock = new ReentrantLock(true);

    private volatile boolean running;

    private final Class<Response> responseClazz = (Class<Response>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];

    @PostConstruct
    private void init() {
        buffer = new LinkedBlockingQueue(getQueueCapacity());
        scheduledTaskService.start(bufferActuatorConfig.getPeriodMillisecond(), this);
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
     * 批处理
     *
     * @param response
     */
    protected abstract void pull(Response response);

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

    @Override
    public Queue getQueue() {
        return buffer;
    }

    @Override
    public int getQueueCapacity() {
        return bufferActuatorConfig.getQueueCapacity();
    }

    @Override
    public boolean offer(BufferRequest request) {
        boolean offer = buffer.offer((Request) request);
        if (!offer) {
            LocalDateTime now = LocalDateTime.now();
            if (null == lastBufferWarningTime) {
                lastBufferWarningTime = now;
            }

            // 3s前有警告时间
            if (now.minusSeconds(3).isAfter(lastBufferWarningTime)) {
                logger.warn("[{}]缓存队列容量已达上限，建议修改参数[dbsyncer.parser.flush.buffer.actuator.queue-capacity={}], ", this.getClass().getSimpleName(), getQueueCapacity());
                lastBufferWarningTime = now;
            }
        }
        return offer;
    }

    @Override
    public void run() {
        if (running) {
            return;
        }

        final Lock bufferLock = lock;
        boolean locked = false;
        try {
            locked = bufferLock.tryLock();
            if (locked) {
                running = true;
                flush(buffer);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (locked) {
                running = false;
                bufferLock.unlock();
            }
        }
    }

    private void flush(Queue<Request> queue) throws IllegalAccessException, InstantiationException {
        if (queue.isEmpty()) {
            return;
        }

        AtomicLong batchCounter = new AtomicLong();
        Map<String, Response> map = new LinkedHashMap<>();
        while (!queue.isEmpty() && batchCounter.get() < bufferActuatorConfig.getBatchCount()) {
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

        map.forEach((key, flushTask) -> {
            long now = Instant.now().toEpochMilli();
            try {
                pull(flushTask);
            } catch (Exception e) {
                logger.error("[{}]异常{}", key);
            }
            final BufferResponse task = (BufferResponse) flushTask;
            logger.info("[{}{}]{}, {}ms", key, task.getSuffixName(), task.getTaskSize(), (Instant.now().toEpochMilli() - now));
        });
        map.clear();
        map = null;
        batchCounter = null;
    }

}
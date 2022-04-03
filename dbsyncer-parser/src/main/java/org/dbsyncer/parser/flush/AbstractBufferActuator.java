package org.dbsyncer.parser.flush;

import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.parser.flush.model.AbstractRequest;
import org.dbsyncer.parser.flush.model.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:36
 */
public abstract class AbstractBufferActuator<Request, Response> implements BufferActuator, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    private Queue<Request> buffer = new ConcurrentLinkedQueue();

    private Queue<Request> temp = new ConcurrentLinkedQueue();

    private final Lock lock = new ReentrantLock(true);

    private volatile boolean running;

    private final static long MAX_BATCH_COUNT = 1000L;

    @PostConstruct
    private void init() {
        scheduledTaskService.start(getPeriod(), this);
    }

    /**
     * 获取定时间隔（毫秒）
     *
     * @return
     */
    protected abstract long getPeriod();

    /**
     * 生成缓存value
     *
     * @return
     */
    protected abstract AbstractResponse getValue();

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

    @Override
    public void offer(AbstractRequest request) {
        if (running) {
            temp.offer((Request) request);
            return;
        }
        buffer.offer((Request) request);
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
                running = false;
                flush(temp);
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

    private void flush(Queue<Request> queue) {
        if (!queue.isEmpty()) {
            AtomicLong batchCounter = new AtomicLong();
            final Map<String, AbstractResponse> map = new LinkedHashMap<>();
            while (!queue.isEmpty() && batchCounter.get() < MAX_BATCH_COUNT) {
                Request poll = queue.poll();
                String key = getPartitionKey(poll);
                if (!map.containsKey(key)) {
                    map.putIfAbsent(key, getValue());
                }
                partition(poll, (Response) map.get(key));
                batchCounter.incrementAndGet();
            }

            map.forEach((key, flushTask) -> {
                long now = Instant.now().toEpochMilli();
                try {
                    pull((Response) flushTask);
                } catch (Exception e) {
                    logger.error("[{}]异常{}", key);
                }
                logger.info("[{}]{}条，耗时{}秒", key, flushTask.getTaskSize(), (Instant.now().toEpochMilli() - now) / 1000);
            });
            map.clear();
        }
    }

}
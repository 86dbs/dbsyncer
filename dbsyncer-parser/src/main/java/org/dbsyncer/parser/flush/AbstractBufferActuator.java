package org.dbsyncer.parser.flush;

import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:36
 */
public abstract class AbstractBufferActuator<B, F> implements BufferActuator, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    private Queue<B> buffer = new ConcurrentLinkedQueue();

    private Queue<B> temp = new ConcurrentLinkedQueue();

    private final Object LOCK = new Object();

    private volatile boolean running;

    @PostConstruct
    private void init() {
        scheduledTaskService.start(300, this);
    }

    /**
     * 生成缓存value
     *
     * @return
     */
    protected abstract AbstractFlushTask getValue();

    /**
     * 生成分区key
     *
     * @param bufferTask
     * @return
     */
    protected abstract String getPartitionKey(B bufferTask);

    /**
     * 分区
     *
     * @param bufferTask
     * @param flushTask
     */
    protected abstract void partition(B bufferTask, F flushTask);

    /**
     * 异步批处理
     *
     * @param flushTask
     */
    protected abstract void flush(F flushTask);

    @Override
    public void offer(AbstractBufferTask task) {
        if (running) {
            temp.offer((B) task);
            return;
        }
        buffer.offer((B) task);
    }

    @Override
    public void run() {
        if (running) {
            return;
        }
        synchronized (LOCK) {
            if (running) {
                return;
            }
            running = true;
            flush(buffer);
            running = false;
            flush(temp);
        }
    }

    private void flush(Queue<B> queue) {
        if (!queue.isEmpty()) {
            final Map<String, AbstractFlushTask> map = new LinkedHashMap<>();
            while (!queue.isEmpty()) {
                B poll = queue.poll();
                String key = getPartitionKey(poll);
                if (!map.containsKey(key)) {
                    map.putIfAbsent(key, getValue());
                }
                partition(poll, (F) map.get(key));
            }

            map.forEach((key, flushTask) -> {
                long now = Instant.now().toEpochMilli();
                try {
                    flush((F) flushTask);
                } catch (Exception e) {
                    logger.error("[{}]-flush异常{}", key);
                }
                logger.info("[{}]-flush{}条，耗时{}秒", key, flushTask.getFlushTaskSize(), (Instant.now().toEpochMilli() - now) / 1000);
            });
            map.clear();
        }
    }

}
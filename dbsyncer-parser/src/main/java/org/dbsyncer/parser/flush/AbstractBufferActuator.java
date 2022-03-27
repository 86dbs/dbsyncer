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

    private final Object LOCK = new Object();

    private volatile boolean running;

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
     * 异步批处理
     *
     * @param response
     */
    protected abstract void flush(Response response);

    @Override
    public void offer(AbstractRequest task) {
        if (running) {
            temp.offer((Request) task);
            return;
        }
        buffer.offer((Request) task);
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

    private void flush(Queue<Request> queue) {
        if (!queue.isEmpty()) {
            final Map<String, AbstractResponse> map = new LinkedHashMap<>();
            while (!queue.isEmpty()) {
                Request poll = queue.poll();
                String key = getPartitionKey(poll);
                if (!map.containsKey(key)) {
                    map.putIfAbsent(key, getValue());
                }
                partition(poll, (Response) map.get(key));
            }

            map.forEach((key, flushTask) -> {
                long now = Instant.now().toEpochMilli();
                try {
                    flush((Response) flushTask);
                } catch (Exception e) {
                    logger.error("[{}]-flush异常{}", key);
                }
                logger.info("[{}]-flush{}条，耗时{}秒", key, flushTask.getTaskSize(), (Instant.now().toEpochMilli() - now) / 1000);
            });
            map.clear();
        }
    }

}
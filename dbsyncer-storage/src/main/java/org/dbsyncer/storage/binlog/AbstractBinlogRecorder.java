package org.dbsyncer.storage.binlog;

import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/8 0:53
 */
public abstract class AbstractBinlogRecorder<Message> implements BinlogRecorder, ScheduledTaskJob, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    private static final long MAX_BATCH_COUNT = 100L;

    private static final long PERIOD = 3000;

    private final Lock lock = new ReentrantLock(true);

    private volatile boolean running;

    private BinlogContext context;

    @PostConstruct
    private void init() throws IOException {
        // /data/binlog/WriterBinlog/
        context = new BinlogContext(getTaskName());
        scheduledTaskService.start(PERIOD, this);
    }

    /**
     * 获取任务名称
     *
     * @return
     */
    protected String getTaskName() {
        return getClass().getSimpleName();
    }

    /**
     * 获取缓存队列
     *
     * @return
     */
    protected abstract Queue getQueue();

    /**
     * 反序列化任务
     *
     * @param message
     * @return
     */
    protected abstract Message deserialize(BinlogMessage message);

    @Override
    public void run() {
        if (running || !getQueue().isEmpty()) {
            return;
        }

        final Lock binlogLock = lock;
        boolean locked = false;
        try {
            locked = binlogLock.tryLock();
            if (locked) {
                running = true;
                doParse();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (locked) {
                running = false;
                binlogLock.unlock();
            }
        }
    }

    @Override
    public void flush(BinlogMessage message) throws IOException {
        context.write(message);
    }

    @Override
    public void destroy() {
        context.close();
    }

    private void doParse() throws IOException {
        byte[] line;
        AtomicInteger batchCounter = new AtomicInteger();
        while (batchCounter.get() < MAX_BATCH_COUNT && null != (line = context.readLine())) {
            deserialize(BinlogMessage.parseFrom(line));
            // getQueue().offer(deserialize(message));
            batchCounter.getAndAdd(1);
        }

        if (batchCounter.get() > 0) {
            context.flush();
        }
    }
}
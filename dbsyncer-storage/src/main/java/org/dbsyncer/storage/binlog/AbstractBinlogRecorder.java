package org.dbsyncer.storage.binlog;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.storage.binlog.impl.BinlogColumnValue;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.Types;
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

    private static final long CONTEXT_PERIOD = 10_000;

    private static final BinlogColumnValue value = new BinlogColumnValue();

    private final Lock lock = new ReentrantLock(true);

    private volatile boolean running;

    private BinlogContext context;

    @PostConstruct
    private void init() throws IOException {
        // /data/binlog/WriterBinlog/
        context = new BinlogContext(getTaskName());
        scheduledTaskService.start(PERIOD, this);
        scheduledTaskService.start(CONTEXT_PERIOD, context);
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
            Message message = deserialize(BinlogMessage.parseFrom(line));
            if (null != message) {
                getQueue().offer(message);
            }
            batchCounter.getAndAdd(1);
        }

        if (batchCounter.get() > 0) {
            context.flush();
        }
    }

    /**
     * Resolve value
     *
     * @param type
     * @param columnValue
     * @return
     */
    protected Object resolveValue(int type, ByteString columnValue) {
        value.setValue(columnValue);

        if (value.isNull()) {
            return null;
        }

        switch (type) {
            // 字符串
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.CHAR:
                return value.asString();

            // 时间
            case Types.TIMESTAMP:
                return value.asTimestamp();
            case Types.DATE:
                return value.asDate();

            // 数字
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                return value.asInteger();
            case Types.BIGINT:
                return value.asLong();
            case Types.FLOAT:
            case Types.REAL:
                return value.asFloat();
            case Types.DOUBLE:
                return value.asDouble();
            case Types.DECIMAL:
            case Types.NUMERIC:
                return value.asDecimal();

            // 布尔
            case Types.BOOLEAN:
                return value.asBoolean();

            // 字节
            case Types.BIT:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return value.asByteArray();

            // TODO 待实现
            case Types.NCLOB:
            case Types.CLOB:
            case Types.BLOB:
                return null;

            // 暂不支持
            case Types.ROWID:
                return null;

            default:
                return null;
        }
    }

}
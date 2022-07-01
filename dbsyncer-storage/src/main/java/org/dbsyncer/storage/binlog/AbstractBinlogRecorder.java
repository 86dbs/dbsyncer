package org.dbsyncer.storage.binlog;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.storage.binlog.impl.BinlogColumnValue;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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

    private final ByteBuffer buffer = ByteBuffer.allocate(8);

    /**
     * Java语言提供了八种基本类型，六种数字类型（四个整数型，两个浮点型），一种字符类型，一种布尔型。
     * <p>
     * <ol>
     *     <li>整数：包括int,short,byte,long</li>
     *     <li>浮点型：float,double</li>
     *     <li>字符：char</li>
     *     <li>布尔：boolean</li>
     * </ol>
     *
     * <pre>
     * 类型     长度     大小      最小值     最大值
     * byte     1Byte    8-bit     -128       +127
     * short    2Byte    16-bit    -2^15      +2^15-1
     * int      4Byte    32-bit    -2^31      +2^31-1
     * long     8Byte    64-bit    -2^63      +2^63-1
     * float    4Byte    32-bit    IEEE754    IEEE754
     * double   8Byte    64-bit    IEEE754    IEEE754
     * char     2Byte    16-bit    Unicode 0  Unicode 2^16-1
     * boolean  8Byte    64-bit
     * </pre>
     *
     * @param v
     * @return
     */
    protected ByteString serializeValue(Object v) {
        String type = v.getClass().getName();
        switch (type) {
            // 字节
//            case "[B":
//            return ByteString.copyFrom((byte[]) v);

            // 字符串
            case "java.lang.String":
                return ByteString.copyFromUtf8((String) v);

            // 时间
            case "java.sql.Timestamp":
                buffer.clear();
                Timestamp timestamp = (Timestamp) v;
                buffer.putLong(timestamp.getTime());
                buffer.flip();
                return ByteString.copyFrom(buffer, 8);
            case "java.sql.Date":
                return ByteString.copyFromUtf8(DateFormatUtil.dateToString((Date) v));
            case "java.sql.Time":
                Time time = (Time) v;
                return ByteString.copyFromUtf8(time.toString());

            // 数字
            case "java.lang.Integer":
                buffer.clear();
                buffer.putInt((Integer) v);
                buffer.flip();
                return ByteString.copyFrom(buffer, 4);
            case "java.lang.Long":
                buffer.clear();
                buffer.putLong((Long) v);
                buffer.flip();
                return ByteString.copyFrom(buffer, 8);
            case "java.lang.Short":
                buffer.clear();
                buffer.putShort((Short) v);
                buffer.flip();
                return ByteString.copyFrom(buffer, 2);
            case "java.lang.Float":
                buffer.clear();
                buffer.putFloat((Float) v);
                buffer.flip();
                return ByteString.copyFrom(buffer, 4);
            case "java.lang.Double":
                buffer.clear();
                buffer.putDouble((Double) v);
                buffer.flip();
                return ByteString.copyFrom(buffer, 8);
            case "java.math.BigDecimal":
                BigDecimal bigDecimal = (BigDecimal) v;
                return ByteString.copyFromUtf8(bigDecimal.toString());

            // 布尔(1为true;0为false)
            case "java.lang.Boolean":
                buffer.clear();
                Boolean b = (Boolean) v;
                buffer.putShort((short) (b ? 1 : 0));
                buffer.flip();
                return ByteString.copyFrom(buffer, 2);

            default:
                logger.error("Unsupported serialize value type:{}", type);
                return null;
        }
    }

    /**
     * Resolve value
     *
     * @param type
     * @param v
     * @return
     */
    protected Object resolveValue(int type, ByteString v) {
        value.setValue(v);

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
            case Types.TIME:
                return value.asTime();
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
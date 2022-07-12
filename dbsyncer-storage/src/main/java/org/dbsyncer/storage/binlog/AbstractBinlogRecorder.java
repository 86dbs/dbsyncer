package org.dbsyncer.storage.binlog;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import oracle.sql.TIMESTAMP;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.snowflake.SnowflakeIdWorker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.storage.binlog.impl.BinlogColumnValue;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.query.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/8 0:53
 */
public abstract class AbstractBinlogRecorder<Message> implements BinlogRecorder, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    @Autowired
    private SnowflakeIdWorker snowflakeIdWorker;

    private static final String BINLOG_ID = "id";

    private static final String BINLOG_CONTENT = "c";

    private static final int SUBMIT_COUNT = 1000;

    private static final Queue<BinlogMessage> queue = new LinkedBlockingQueue(10000);

    private static final ByteBuffer buffer = ByteBuffer.allocate(8);

    private static final BinlogColumnValue value = new BinlogColumnValue();

    private static final String PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar)
            .append("data").append(File.separatorChar).append("data").append(File.separatorChar).toString();

    private Shard shard;

    private WriterTask writerTask = new WriterTask();

    private ReaderTask readerTask = new ReaderTask();

    @PostConstruct
    private void init() throws IOException {
        // /data/data/WriterBinlog/
        shard = new Shard(PATH + getTaskName());
        scheduledTaskService.start(500, writerTask);
        scheduledTaskService.start(2000, readerTask);
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
     * 反序列化任务
     *
     * @param message
     * @return
     */
    protected abstract Message deserialize(BinlogMessage message);

    @Override
    public void flush(BinlogMessage message) {
        queue.offer(message);
    }

    @Override
    public void destroy() throws IOException {
        shard.close();
    }

    private void doParse() throws IOException {
        Option option = new Option(new MatchAllDocsQuery());
        option.addIndexFieldResolverEnum(BINLOG_CONTENT, IndexFieldResolverEnum.BINARY);
        Paging paging = shard.query(option, 1, SUBMIT_COUNT, null);
        if (CollectionUtils.isEmpty(paging.getData())) {
            return;
        }

        List<Map> list = (List<Map>) paging.getData();
        int size = list.size();
        Term[] terms = new Term[size];
        for (int i = 0; i < size; i++) {
            try {
                BytesRef ref = (BytesRef) list.get(i).get(BINLOG_CONTENT);
                Message message = deserialize(BinlogMessage.parseFrom(ref.bytes));
                if (null != message) {
                    getQueue().offer(message);
                }
                terms[i] = new Term(BINLOG_ID, (String) list.get(i).get(BINLOG_ID));
            } catch (InvalidProtocolBufferException e) {
                logger.error(e.getMessage());
            }
        }
        shard.deleteBatch(terms);
    }

    /**
     * Java语言提供了八种基本类型，六种数字类型（四个整数型，两个浮点型），一种字符类型，一种布尔型。
     * <p>
     * <ol>
     * <li>整数：包括int,short,byte,long</li>
     * <li>浮点型：float,double</li>
     * <li>字符：char</li>
     * <li>布尔：boolean</li>
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
            case "[B":
                return ByteString.copyFrom((byte[]) v);

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
                buffer.clear();
                Date date = (Date) v;
                buffer.putLong(date.getTime());
                buffer.flip();
                return ByteString.copyFrom(buffer, 8);
            case "java.sql.Time":
                buffer.clear();
                Time time = (Time) v;
                buffer.putLong(time.getTime());
                buffer.flip();
                return ByteString.copyFrom(buffer, 8);

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
            case "java.util.BitSet":
                BitSet bitSet = (BitSet) v;
                return ByteString.copyFrom(bitSet.toByteArray());

            // 布尔(1为true;0为false)
            case "java.lang.Boolean":
                buffer.clear();
                Boolean b = (Boolean) v;
                buffer.putShort((short) (b ? 1 : 0));
                buffer.flip();
                return ByteString.copyFrom(buffer, 2);
            case "oracle.sql.TIMESTAMP":
                buffer.clear();
                TIMESTAMP timeStamp = (TIMESTAMP) v;
                try {
                    buffer.putLong(timeStamp.timestampValue().getTime());
                } catch (SQLException e) {
                    logger.error(e.getMessage());
                }
                buffer.flip();
                return ByteString.copyFrom(buffer, 8);
            case "oracle.sql.BLOB":
                BLOB blob = (BLOB) v;
                return ByteString.copyFrom(getBytes(blob));
            case "oracle.sql.CLOB":
                CLOB clob = (CLOB) v;
                return ByteString.copyFrom(getBytes(clob));

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
                return value.asInteger();
            case Types.SMALLINT:
                return value.asShort();
            case Types.BIGINT:
                return value.asLong();
            case Types.FLOAT:
            case Types.REAL:
                return value.asFloat();
            case Types.DOUBLE:
                return value.asDouble();
            case Types.DECIMAL:
            case Types.NUMERIC:
                return value.asBigDecimal();

            // 布尔
            case Types.BOOLEAN:
                return value.asBoolean();

            // 字节
            case Types.BIT:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return value.asByteArray();

            // 二进制对象
            case Types.NCLOB:
            case Types.CLOB:
            case Types.BLOB:
                return value.asByteArray();

            // 暂不支持
            case Types.ROWID:
                return null;

            default:
                return null;
        }
    }

    /**
     * 合并缓存队列任务到磁盘
     */
    final class WriterTask implements ScheduledTaskJob {

        @Override
        public void run() {
            if (queue.isEmpty()) {
                return;
            }

            List<Document> tasks = new ArrayList<>();
            int count = 0;
            Document doc;
            while (!queue.isEmpty() && count < SUBMIT_COUNT) {
                BinlogMessage message = queue.poll();
                if (null != message) {
                    doc = new Document();
                    doc.add(new StringField(BINLOG_ID, String.valueOf(snowflakeIdWorker.nextId()), Field.Store.YES));
                    doc.add(new StoredField(BINLOG_CONTENT, new BytesRef(message.toByteArray())));
                    tasks.add(doc);
                }
                count++;
            }

            if (!CollectionUtils.isEmpty(tasks)) {
                try {
                    shard.insertBatch(tasks);
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    /**
     * 从磁盘读取日志到任务队列
     */
    final class ReaderTask implements ScheduledTaskJob {

        private final Lock lock = new ReentrantLock(true);

        private volatile boolean running;

        @Override
        public void run() {
            if (running || (SUBMIT_COUNT * 2) + getQueue().size() >= getQueueCapacity()) {
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
    }

    private byte[] getBytes(BLOB blob) {
        InputStream is = null;
        byte[] b = null;
        try {
            is = blob.getBinaryStream();
            b = new byte[(int) blob.length()];
            is.read(b);
            return b;
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            IOUtils.closeQuietly(is);
        }
        return b;
    }

    private byte[] getBytes(CLOB clob) {
        InputStream is = null;
        byte[] b = null;
        try {
            is = clob.binaryStreamValue();
            b = new byte[(int) clob.length()];
            is.read(b);
            return b;
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            IOUtils.closeQuietly(is);
        }
        return b;
    }

}
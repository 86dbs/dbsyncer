package org.dbsyncer.storage.binlog;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.dbsyncer.common.config.BinlogRecorderConfig;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.snowflake.SnowflakeIdWorker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.constant.BinlogConstant;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.query.Option;
import org.dbsyncer.storage.util.DocumentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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

    private static final String PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("data").append(File.separatorChar).append("data").append(File.separatorChar).toString();

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    @Autowired
    private SnowflakeIdWorker snowflakeIdWorker;

    @Autowired
    private BinlogRecorderConfig binlogRecorderConfig;

    private static Queue<BinlogMessage> queue;

    private static Shard shard;

    private WriterTask writerTask = new WriterTask();

    private ReaderTask readerTask = new ReaderTask();

    @PostConstruct
    private void init() throws IOException {
        queue = new LinkedBlockingQueue(binlogRecorderConfig.getQueueCapacity());
        shard = new Shard(PATH + getTaskName());
        scheduledTaskService.start(binlogRecorderConfig.getWriterPeriodMillisecond(), writerTask);
        scheduledTaskService.start(binlogRecorderConfig.getReaderPeriodMillisecond(), readerTask);
    }

    /**
     * 反序列化消息
     *
     * @param message
     * @return
     */
    protected abstract Message deserialize(String messageId, BinlogMessage message);

    @Override
    public void flush(BinlogMessage message) {
        queue.offer(message);
    }

    @Override
    public void destroy() throws IOException {
        shard.close();
    }

    @Override
    public void complete(List<String> messageIds) {
        if (!CollectionUtils.isEmpty(messageIds)) {
            try {
                int size = messageIds.size();
                Term[] terms = new Term[size];
                for (int i = 0; i < size; i++) {
                    terms[i] = new Term(BinlogConstant.BINLOG_ID, messageIds.get(i));
                }
                shard.deleteBatch(terms);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
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
            long now = Instant.now().toEpochMilli();
            while (!queue.isEmpty() && count < binlogRecorderConfig.getBatchCount()) {
                BinlogMessage message = queue.poll();
                if (null != message) {
                    tasks.add(DocumentUtil.convertBinlog2Doc(String.valueOf(snowflakeIdWorker.nextId()), BinlogConstant.READY, new BytesRef(message.toByteArray()), now));
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
            if (running || (binlogRecorderConfig.getBatchCount() * 2) + getQueue().size() >= getQueueCapacity()) {
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

        private void doParse() throws IOException {
            //  查询[待处理] 或 [处理中 & 处理超时]
            long maxProcessingSeconds = Timestamp.valueOf(LocalDateTime.now().minusSeconds(binlogRecorderConfig.getMaxProcessingSeconds())).getTime();
            BooleanQuery query = new BooleanQuery.Builder()
                    .add(new BooleanQuery.Builder()
                            .add(IntPoint.newExactQuery(BinlogConstant.BINLOG_STATUS, BinlogConstant.READY), BooleanClause.Occur.MUST)
                            .build(), BooleanClause.Occur.SHOULD)
                    .add(new BooleanQuery.Builder()
                            .add(IntPoint.newExactQuery(BinlogConstant.BINLOG_STATUS, BinlogConstant.PROCESSING), BooleanClause.Occur.MUST)
                            .add(LongPoint.newRangeQuery(BinlogConstant.BINLOG_TIME, Long.MIN_VALUE, maxProcessingSeconds), BooleanClause.Occur.MUST)
                            .build(), BooleanClause.Occur.SHOULD)
                    .build();
            Option option = new Option(query);
            option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_STATUS, IndexFieldResolverEnum.INT);
            option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_CONTENT, IndexFieldResolverEnum.BINARY);
            option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_TIME, IndexFieldResolverEnum.LONG);

            // 优先处理最早记录
            Sort sort = new Sort(new SortField(BinlogConstant.BINLOG_TIME, SortField.Type.LONG));
            Paging paging = shard.query(option, 1, binlogRecorderConfig.getBatchCount(), sort);
            if (CollectionUtils.isEmpty(paging.getData())) {
                return;
            }

            List<Map> list = (List<Map>) paging.getData();
            final int size = list.size();
            final List<Message> messages = new ArrayList<>(size);
            final List<Document> updateDocs = new ArrayList<>(size);
            final Term[] deleteIds = new Term[size];
            for (int i = 0; i < size; i++) {
                Map row = list.get(i);
                String id = (String) row.get(BinlogConstant.BINLOG_ID);
                Integer status = (Integer) row.get(BinlogConstant.BINLOG_STATUS);
                BytesRef ref = (BytesRef) row.get(BinlogConstant.BINLOG_CONTENT);
                if (BinlogConstant.PROCESSING == status) {
                    logger.warn("存在超时未处理数据，正在重试，建议优化配置参数[max-processing-seconds={}].", binlogRecorderConfig.getMaxProcessingSeconds());
                }
                deleteIds[i] = new Term(BinlogConstant.BINLOG_ID, id);
                String newId = String.valueOf(snowflakeIdWorker.nextId());
                try {
                    Message message = deserialize(newId, BinlogMessage.parseFrom(ref.bytes));
                    if (null != message) {
                        messages.add(message);
                        updateDocs.add(DocumentUtil.convertBinlog2Doc(newId, BinlogConstant.PROCESSING, ref, Instant.now().toEpochMilli()));
                    }
                } catch (InvalidProtocolBufferException e) {
                    logger.error(e.getMessage());
                }
            }

            // 如果在更新消息状态的过程中服务被中止，为保证数据的安全性，重启后消息可能会同步2次）
            shard.insertBatch(updateDocs);
            shard.deleteBatch(deleteIds);
            getQueue().addAll(messages);
        }
    }

}
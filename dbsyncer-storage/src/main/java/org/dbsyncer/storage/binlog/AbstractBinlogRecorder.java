package org.dbsyncer.storage.binlog;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.OrFileFilter;
import org.dbsyncer.common.file.BufferedRandomAccessFile;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
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

    private static final long BINLOG_MAX_SIZE = 256 * 1024 * 1024;

    private static final int BINLOG_EXPIRE_DAYS = 7;

    private static final long MAX_BATCH_COUNT = 100L;

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private static final String BINLOG = "binlog";

    private static final String BINLOG_INDEX = BINLOG + ".index";

    private static final String BINLOG_CONFIG = BINLOG + ".config";

    private static final long PERIOD = 3000;

    private final Lock lock = new ReentrantLock(true);

    private volatile boolean running;

    private String path;

    private File configFile;

    private File binlogFile;

    private File indexFile;

    private Binlog binlog;

    private BinlogPipeline pipeline;

    @PostConstruct
    private void init() throws IOException {
        // /data/binlog/{BufferActuator}/
        path = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar)
                .append("data").append(File.separatorChar)
                .append("binlog").append(File.separatorChar)
                .append(getTaskName()).append(File.separatorChar)
                .toString();
        File dir = new File(path);
        if (!dir.exists()) {
            FileUtils.forceMkdir(dir);
        }

        // binlog.config
        configFile = new File(path + BINLOG_CONFIG);
        // binlog.index
        indexFile = new File(path + BINLOG_INDEX);
        if (!configFile.exists()) {
            initBinlog(createBinlogName(0));
            initBinlogIndex();
        }
        discardExpiredFile();

        if (null == binlog) {
            binlog = JsonUtil.jsonToObj(FileUtils.readFileToString(configFile, Charset.defaultCharset()), Binlog.class);
        }
        binlogFile = new File(path + binlog.getBinlog());
        if (!binlogFile.exists()) {
            logger.warn("The binlogFile '{}' is not exist.", binlogFile.getAbsolutePath());
            initBinlog(createBinlogName(getBinlogIndex(binlog.getBinlog())));
            initBinlogIndex();
        }

        initPipeline();
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
                discardExpiredFile();
                switchFile();
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
    public void flush(BinlogMessage message) {
        if (null != message) {
            try {
                pipeline.write(message.toByteArray());
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public void destroy() {
        IOUtils.closeQuietly(pipeline);
    }

    private void doParse() throws IOException {
        byte[] line;
        AtomicInteger batchCounter = new AtomicInteger();
        while (batchCounter.get() < MAX_BATCH_COUNT && null != (line = pipeline.readLine())) {
            deserialize(BinlogMessage.parseFrom(line));
            //                getQueue().offer(deserialize(message));
            batchCounter.getAndAdd(1);
        }

        if (batchCounter.get() > 0) {
            binlog.setPos(pipeline.getFilePointer());
            FileUtils.writeStringToFile(configFile, JsonUtil.objToJson(binlog), DEFAULT_CHARSET);
        }
    }

    private void switchFile() throws IOException {
        if(binlogFile.length() > BINLOG_MAX_SIZE){
            // fixme 锁上下文

            List<String> list = FileUtils.readLines(indexFile, DEFAULT_CHARSET);
            int index = 1;
            if (!CollectionUtils.isEmpty(list)) {
                index = getBinlogIndex(list.get(list.size() - 1));
            }
            String newBinlogName = createBinlogName(index);
            FileUtils.write(indexFile, newBinlogName + LINE_SEPARATOR, DEFAULT_CHARSET, true);
            IOUtils.closeQuietly(pipeline);
            initBinlog(newBinlogName);
            initPipeline();
        }
    }

    private void discardExpiredFile() throws IOException {
        List<String> index = FileUtils.readLines(indexFile, DEFAULT_CHARSET);
        if (!CollectionUtils.isEmpty(index)) {
            Set<String> shouldDelete = new HashSet<>();
            for (String i : index) {
                File file = new File(path + i);
                if (!file.exists()) {
                    shouldDelete.add(i);
                    continue;
                }
                if (isExpiredFile(file)) {
                    FileUtils.forceDelete(file);
                    shouldDelete.add(i);
                }
            }
            if (!CollectionUtils.isEmpty(shouldDelete)) {
                StringBuilder indexBuffer = new StringBuilder();
                index.forEach(i -> {
                    if (!shouldDelete.contains(i)) {
                        indexBuffer.append(i).append(LINE_SEPARATOR);
                    }
                });
                FileUtils.writeStringToFile(indexFile, indexBuffer.toString(), DEFAULT_CHARSET);
            }
        }
    }

    private boolean isExpiredFile(File file) throws IOException {
        BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
        Instant instant = attr.creationTime().toInstant();
        return Timestamp.from(instant).getTime() < Timestamp.valueOf(LocalDateTime.now().minusDays(BINLOG_EXPIRE_DAYS)).getTime();
    }

    private void initBinlog(String binlogName) throws IOException {
        // binlog.config
        binlog = new Binlog().setBinlog(binlogName);
        FileUtils.writeStringToFile(configFile, JsonUtil.objToJson(binlog), DEFAULT_CHARSET);

        // binlog.000001
        binlogFile = new File(path + binlogName);
        FileUtils.writeStringToFile(binlogFile, "", DEFAULT_CHARSET);
    }

    private void initBinlogIndex() throws IOException {
        // binlog.index
        FileUtils.writeStringToFile(indexFile, binlog.getBinlog() + LINE_SEPARATOR, DEFAULT_CHARSET);
    }

    private void initPipeline() throws IOException {
        final RandomAccessFile raf = new BufferedRandomAccessFile(binlogFile, "rwd");
        raf.seek(binlog.getPos());
        pipeline = new BinlogPipeline(raf);
    }

    private String createBinlogName(int index) {
        return String.format("%s.%06d", BINLOG, index <= 0 ? 1 : index + 1);
    }

    private int getBinlogIndex(String binlogName) {
        return NumberUtil.toInt(StringUtil.substring(binlogName, BINLOG.length() + 1));
    }

}
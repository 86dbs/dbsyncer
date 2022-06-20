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
import java.util.Collection;
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

    private static final long BINLOG_MAX_SIZE = 512 * 1024 * 1024;

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

    private File configPath;

    private File binlogFile;

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
        configPath = new File(path + BINLOG_CONFIG);

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
            FileUtils.writeStringToFile(configPath, JsonUtil.objToJson(binlog), DEFAULT_CHARSET);
        }
    }

    private void switchFile() {
        // fixme 切换新文件
    }

    private void createNewBinlog() {
        Collection<File> files = getBinlogIndexFileList();
        int index = 1;
        if (CollectionUtils.isEmpty(files)) {
            for (File file : files) {
                String binlogName = file.getName();
                int i = getBinlogNameIndex(binlogName);
                index = i > index ? i : index;
            }
        }
    }

    private void discardExpiredFile() throws IOException {
        Collection<File> files = getBinlogIndexFileList();
        if (CollectionUtils.isEmpty(files)) {
            for (File file : files) {
                if (isExpiredFile(file)) {
                    FileUtils.forceDelete(file);
                }
            }
        }
    }

    private boolean isExpiredFile(File file) throws IOException {
        BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
        Instant instant = attr.creationTime().toInstant();
        return Timestamp.from(instant).getTime() < Timestamp.valueOf(LocalDateTime.now().minusDays(BINLOG_EXPIRE_DAYS)).getTime();
    }

    private Collection<File> getBinlogIndexFileList() {
        File dir = new File(path + BINLOG_CONFIG);
        OrFileFilter filter = new OrFileFilter();
        filter.addFileFilter(FileFilterUtils.nameFileFilter(BINLOG_INDEX));
        filter.addFileFilter(FileFilterUtils.nameFileFilter(BINLOG_CONFIG));
        Collection<File> files = FileUtils.listFiles(dir, FileFilterUtils.notFileFilter(filter), null);
        return files;
    }

    private void initBinlog(String binlogName) throws IOException {
        // binlog.config
        binlog = new Binlog().setBinlog(binlogName);
        FileUtils.writeStringToFile(configPath, JsonUtil.objToJson(binlog), DEFAULT_CHARSET);

        // binlog.000001
        binlogFile = new File(path + binlogName);
        FileUtils.writeStringToFile(binlogFile, "", DEFAULT_CHARSET);

        // binlog.index
        FileUtils.writeStringToFile(new File(path + BINLOG_INDEX), binlogName + LINE_SEPARATOR, DEFAULT_CHARSET);
    }

    private void initPipeline() throws IOException {
        if (!configPath.exists()) {
            initBinlog(createBinlogName(1));
        }
        discardExpiredFile();
        createNewBinlog();

        if (null == binlog) {
            binlog = JsonUtil.jsonToObj(FileUtils.readFileToString(configPath, Charset.defaultCharset()), Binlog.class);
        }
        binlogFile = new File(path + binlog.getBinlog());
        if (!binlogFile.exists()) {
            logger.warn("The binlogFile '{}' is not exist.", binlogFile.getAbsolutePath());
            initBinlog(createBinlogName(binlog.getBinlog()));
        }

        final RandomAccessFile raf = new BufferedRandomAccessFile(binlogFile, "rwd");
        raf.seek(binlog.getPos());
        pipeline = new BinlogPipeline(raf);
    }

    private String createBinlogName(int index) {
        return String.format("%s.%06d", BINLOG, index <= 0 ? 1 : index);
    }

    private String createBinlogName(String binlogName) {
        return createBinlogName(getBinlogNameIndex(binlogName) + 1);
    }

    private int getBinlogNameIndex(String binlogName) {
        return NumberUtil.toInt(StringUtil.substring(binlogName, BINLOG.length() + 1));
    }

}
package org.dbsyncer.storage.binlog;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.storage.binlog.impl.BinlogPipeline;
import org.dbsyncer.storage.binlog.impl.BinlogReader;
import org.dbsyncer.storage.binlog.impl.BinlogWriter;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.model.BinlogConfig;
import org.dbsyncer.storage.model.BinlogIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>组件介绍</p>
 * <ol>
 *     <li>BinlogPipeline（提供文件流读写）</li>
 *     <li>定时器（维护索引文件状态，回收文件流）</li>
 *     <li>索引</li>
 * </ol>
 * <p>定时器</p>
 * <ol>
 *     <li>生成新索引（超过限制大小｜过期）</li>
 *     <li>关闭索引流（有锁 & 读写状态关闭 & 30s未用）</li>
 *     <li>删除旧索引（无锁 & 过期）</li>
 * </ol>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/29 1:28
 */
public class BinlogContext implements ScheduledTaskJob, Closeable {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final long BINLOG_MAX_SIZE = 256 * 1024 * 1024;

    private static final int BINLOG_EXPIRE_DAYS = 7;

    private static final int BINLOG_ACTUATOR_CLOSE_DELAYED_SECONDS = 30;

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private static final String BINLOG = "binlog";

    private static final String BINLOG_INDEX = BINLOG + ".index";

    private static final String BINLOG_CONFIG = BINLOG + ".config";

    private final List<BinlogIndex> indexList = new LinkedList<>();

    private final String path;

    private final File configFile;

    private final File indexFile;

    private final BinlogPipeline pipeline;

    private final Lock readerLock = new ReentrantLock(true);

    private final Lock lock = new ReentrantLock(true);

    private volatile boolean running;

    private BinlogConfig config;

    public BinlogContext(String taskName) throws IOException {
        path = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar)
                .append("data").append(File.separatorChar)
                .append("binlog").append(File.separatorChar)
                .append(taskName).append(File.separatorChar)
                .toString();
        File dir = new File(path);
        if (!dir.exists()) {
            FileUtils.forceMkdir(dir);
        }

        // binlog.index
        indexFile = new File(path + BINLOG_INDEX);
        // binlog.config
        configFile = new File(path + BINLOG_CONFIG);
        if (!configFile.exists()) {
            // binlog.000001
            config = initBinlogConfigAndIndex(createNewBinlogName(0));
        }

        // read index
        Assert.isTrue(indexFile.exists(), String.format("The index file '%s' is not exist.", indexFile.getName()));
        readIndexFromDisk();

        // delete index file
        deleteExpiredIndexFile();

        // {"binlog":"binlog.000001","pos":0}
        if (null == config) {
            config = JsonUtil.jsonToObj(FileUtils.readFileToString(configFile, DEFAULT_CHARSET), BinlogConfig.class);
        }

        // no index
        if (CollectionUtils.isEmpty(indexList)) {
            // binlog.000002
            config = initBinlogConfigAndIndex(createNewBinlogName(config.getFileName()));
            readIndexFromDisk();
        }

        // 配置文件已失效，取最早的索引文件
        BinlogIndex startBinlogIndex = getBinlogIndexByFileName(config.getFileName());
        if (null == startBinlogIndex) {
            logger.warn("The binlog file '{}' is expired.", config.getFileName());
            startBinlogIndex = indexList.get(0);
            config = new BinlogConfig().setFileName(startBinlogIndex.getFileName());
            write(configFile, JsonUtil.objToJson(config), false);
        }

        final BinlogWriter binlogWriter = new BinlogWriter(path, indexList.get(indexList.size() - 1));
        final BinlogReader binlogReader = new BinlogReader(path, startBinlogIndex, config.getPosition());
        pipeline = new BinlogPipeline(binlogWriter, binlogReader);
        logger.info("BinlogContext initialized with config:{}", JsonUtil.objToJson(config));
    }

    @Override
    public void run() {
        if (running) {
            return;
        }

        final Lock binlogLock = lock;
        boolean locked = false;
        try {
            locked = binlogLock.tryLock();
            if (locked) {
                running = true;
                createNewBinlogIndex();
                closeFreeBinlogIndex();
                deleteOldBinlogIndex();
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
    public void close() {
        pipeline.close();
    }

    public void flush() throws IOException {
        config.setFileName(pipeline.getReaderFileName());
        config.setPosition(pipeline.getReaderOffset());
        write(configFile, JsonUtil.objToJson(config), false);
    }

    public byte[] readLine() throws IOException {
        byte[] line = pipeline.readLine();
        if(null == line){
            switchNextBinlogIndex();
        }
        return line;
    }

    public void write(BinlogMessage message) throws IOException {
        pipeline.write(message);
    }

    public BinlogIndex getBinlogIndexByFileName(String fileName) {
        BinlogIndex index = null;
        for (BinlogIndex binlogIndex : indexList) {
            if (StringUtil.equals(binlogIndex.getFileName(), fileName)) {
                index = binlogIndex;
                break;
            }
        }
        return index;
    }

    private void switchNextBinlogIndex() {
        // 有新索引文件
        if(!isCreatedNewBinlogIndex()){
            return;
        }
        boolean locked = false;
        try {
            locked = readerLock.tryLock();
            if (locked) {
                // 有新索引文件
                if(isCreatedNewBinlogIndex()){
                    String newBinlogName = createNewBinlogName(pipeline.getReaderFileName());
                    BinlogIndex startBinlogIndex = getBinlogIndexByFileName(newBinlogName);
                    final BinlogReader binlogReader = pipeline.getBinlogReader();
                    config = new BinlogConfig().setFileName(startBinlogIndex.getFileName());
                    write(configFile, JsonUtil.objToJson(config), false);
                    pipeline.setBinlogReader(new BinlogReader(path, startBinlogIndex, config.getPosition()));
                    binlogReader.stop();
                    logger.info("Switch to new index file '{}' for binlog reader.", newBinlogName);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (locked) {
                readerLock.unlock();
            }
        }
    }

    private boolean isCreatedNewBinlogIndex() {
        return !StringUtil.equals(pipeline.getReaderFileName(), pipeline.getWriterFileName());
    }

    private void createNewBinlogIndex() throws IOException {
        final String writerFileName = pipeline.getWriterFileName();
        File file = new File(path + writerFileName);
        // 超过限制大小｜过期
        if (file.length() > BINLOG_MAX_SIZE || isExpiredFile(file)) {
            final BinlogWriter binlogWriter = pipeline.getBinlogWriter();
            String newBinlogName = createNewBinlogName(writerFileName);
            logger.info("The size of index file '{}' has reached {}MB, exceeding the limit of {}MB, switching to a new index file '{}' for index writer.", writerFileName, getMB(file.length()), getMB(BINLOG_MAX_SIZE), newBinlogName);
            write(indexFile, newBinlogName + LINE_SEPARATOR, true);
            File binlogIndexFile = new File(path + newBinlogName);
            write(binlogIndexFile, "", false);
            BinlogIndex newBinlogIndex = new BinlogIndex(newBinlogName, getFileCreateDateTime(binlogIndexFile));
            indexList.add(newBinlogIndex);
            pipeline.setBinlogWriter(new BinlogWriter(path, newBinlogIndex));
            binlogWriter.stop();
        }
    }

    private void closeFreeBinlogIndex() throws IOException {
        Iterator<BinlogIndex> iterator = indexList.iterator();
        while (iterator.hasNext()) {
            BinlogIndex next = iterator.next();
            // 有锁 & 读写状态关闭 & 30s未用
            if (!next.isFreeLock() && !next.isRunning() && next.getUpdateTime().isBefore(LocalDateTime.now().minusSeconds(BINLOG_ACTUATOR_CLOSE_DELAYED_SECONDS))) {
                next.removeAllLock();
                logger.info("Close free index file '{}', the last update time is {}", next.getFileName(), next.getUpdateTime());
            }
        }
    }

    private void deleteOldBinlogIndex() throws IOException {
        Iterator<BinlogIndex> iterator = indexList.iterator();
        while (iterator.hasNext()) {
            BinlogIndex next = iterator.next();
            // 无锁 & 过期
            if (next.isFreeLock()) {
                File file = new File(path + next.getFileName());
                if(isExpiredFile(file)){
                    FileUtils.forceDelete(file);
                    iterator.remove();
                }
            }
        }
    }

    private long getMB(long size) {
        return size / 1024 / 1024;
    }

    private void readIndexFromDisk() throws IOException {
        indexList.clear();
        List<String> indexNames = FileUtils.readLines(indexFile, DEFAULT_CHARSET);
        if (!CollectionUtils.isEmpty(indexNames)) {
            for (String indexName : indexNames) {
                File file = new File(path + indexName);
                if(file.exists()){
                    indexList.add(new BinlogIndex(indexName, getFileCreateDateTime(file)));
                }
            }
        }
    }

    private BinlogConfig initBinlogConfigAndIndex(String binlogName) throws IOException {
        BinlogConfig config = new BinlogConfig().setFileName(binlogName);
        write(configFile, JsonUtil.objToJson(config), false);
        write(indexFile, binlogName + LINE_SEPARATOR, false);
        write(new File(path + binlogName), "", false);
        return config;
    }

    private void deleteExpiredIndexFile() throws IOException {
        if (CollectionUtils.isEmpty(indexList)) {
            return;
        }
        boolean delete = false;
        Iterator<BinlogIndex> iterator = indexList.iterator();
        while (iterator.hasNext()) {
            BinlogIndex next = iterator.next();
            if (null == next) {
                continue;
            }
            File file = new File(path + next.getFileName());
            if (!file.exists()) {
                logger.info("Delete invalid binlog file '{}'.", next.getFileName());
                iterator.remove();
                delete = true;
                continue;
            }
            if (isExpiredFile(file)) {
                FileUtils.forceDelete(file);
                iterator.remove();
                delete = true;
                logger.info("Delete expired binlog file '{}'.", next.getFileName());
            }
        }

        if (delete) {
            StringBuilder indexBuilder = new StringBuilder();
            indexList.forEach(i -> indexBuilder.append(i.getFileName()).append(LINE_SEPARATOR));
            write(indexFile, indexBuilder.toString(), false);
        }
    }

    private boolean isExpiredFile(File file) throws IOException {
        final LocalDateTime createTime = getFileCreateDateTime(file);
        return createTime.isBefore(LocalDateTime.now().minusDays(BINLOG_EXPIRE_DAYS));
    }

    private LocalDateTime getFileCreateDateTime(File file) throws IOException {
        BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
        return attr.creationTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    private String createNewBinlogName(int index) {
        return String.format("%s.%06d", BINLOG, index % 999999 + 1);
    }

    private String createNewBinlogName(String binlogName) {
        return createNewBinlogName(NumberUtil.toInt(StringUtil.substring(binlogName, BINLOG.length() + 1)));
    }

    private void write(File file, String line, boolean append) throws IOException {
        FileUtils.write(file, line, DEFAULT_CHARSET, append);
    }
}
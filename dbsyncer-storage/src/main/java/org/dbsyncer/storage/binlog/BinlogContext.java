package org.dbsyncer.storage.binlog;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class BinlogContext implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final long BINLOG_MAX_SIZE = 256 * 1024 * 1024;

    private static final int BINLOG_EXPIRE_DAYS = 7;

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private static final String BINLOG = "binlog";

    private static final String BINLOG_INDEX = BINLOG + ".index";

    private static final String BINLOG_CONFIG = BINLOG + ".config";

    private List<String> index = new LinkedList<>();

    private String path;

    private File configFile;

    private File indexFile;

    private File binlogFile;

    private Binlog binlog;

    private BinlogPipeline pipeline;

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

        // binlog.config
        configFile = new File(path + BINLOG_CONFIG);
        if (!configFile.exists()) {
            // binlog.000001
            binlog = new Binlog().setBinlog(createNewBinlogName(0));
            write(configFile, JsonUtil.objToJson(binlog), false);

            binlogFile = new File(path + binlog.getBinlog());
            write(binlogFile, "", false);
        }
        binlog = JsonUtil.jsonToObj(FileUtils.readFileToString(configFile, DEFAULT_CHARSET), Binlog.class);

        // binlog.index
        indexFile = new File(path + BINLOG_INDEX);
        if (!indexFile.exists()) {
            write(indexFile, binlog.getBinlog() + LINE_SEPARATOR, false);
        }

        // binlog.000001
        binlogFile = new File(path + binlog.getBinlog());
        if (!binlogFile.exists()) {
            // 过期/误删
            logger.warn("The binlog file '{}' is expired or deleted.", binlog.getBinlog());

            // binlog.000002
            binlog = new Binlog().setBinlog(createNewBinlogName(getBinlogIndex(binlog.getBinlog())));
            binlogFile = new File(path + binlog.getBinlog());
            write(binlogFile, "", false);
            write(indexFile, binlog.getBinlog() + LINE_SEPARATOR, true);
        }

        // read index
        List<String> indexFileNames = FileUtils.readLines(indexFile, DEFAULT_CHARSET);
        Assert.notEmpty(indexFileNames, String.format("Could not read index from file '%s'", indexFile.getName()));
        index.addAll(indexFileNames);

        // expired file
        discardExpiredFile();

        initPipeline();
    }

    private void switchFile() throws IOException {
        if (binlogFile.length() > BINLOG_MAX_SIZE) {
            int i = 1;
            if (!CollectionUtils.isEmpty(index)) {
                i = getBinlogIndex(index.get(index.size() - 1));
            }
            String newBinlogName = createNewBinlogName(i);
            index.add(newBinlogName);
            FileUtils.write(new File(path + BINLOG_INDEX), newBinlogName + LINE_SEPARATOR, DEFAULT_CHARSET, true);
            // fixme 锁上下文
            pipeline.close();
            pipeline = new BinlogPipeline(binlogFile, binlog.getPos());
        }
    }

    private void discardExpiredFile() throws IOException {
        Set<String> shouldDelete = new HashSet<>();
        for (String name : index) {
            File file = new File(path + name);
            if (!file.exists()) {
                shouldDelete.add(name);
                continue;
            }
            if (isExpiredFile(file)) {
                FileUtils.forceDelete(file);
                shouldDelete.add(name);
            }
        }
        if (!CollectionUtils.isEmpty(shouldDelete)) {
            StringBuilder indexBuffer = new StringBuilder();
            index.forEach(name -> {
                if (!shouldDelete.contains(name)) {
                    indexBuffer.append(name).append(LINE_SEPARATOR);
                }
            });
            write(indexFile, indexBuffer.toString(), false);
        }
    }

    private boolean isExpiredFile(File file) throws IOException {
        BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
        Instant instant = attr.creationTime().toInstant();
        return Timestamp.from(instant).getTime() < Timestamp.valueOf(LocalDateTime.now().minusDays(BINLOG_EXPIRE_DAYS)).getTime();
    }

    private void initPipeline() throws IOException {
        pipeline = new BinlogPipeline(binlogFile, binlog.getPos());
    }

    private String createNewBinlogName(int index) {
        return String.format("%s.%06d", BINLOG, index <= 0 ? 1 : index + 1);
    }

    private int getBinlogIndex(String binlogName) {
        return NumberUtil.toInt(StringUtil.substring(binlogName, BINLOG.length() + 1));
    }

    /**
     * 持久化增量点
     *
     * @throws IOException
     */
    public void flush() throws IOException {
        binlog.setBinlog(pipeline.getBinlogName());
        binlog.setPos(pipeline.getOffset());
        FileUtils.writeStringToFile(configFile, JsonUtil.objToJson(binlog), DEFAULT_CHARSET);
    }

    public BinlogPipeline getBinlogPipeline() {
        return pipeline;
    }

    private void write(File file, String line, boolean append) throws IOException {
        FileUtils.write(file, line, DEFAULT_CHARSET, append);
    }

    @Override
    public void close() {
        pipeline.close();
    }
}
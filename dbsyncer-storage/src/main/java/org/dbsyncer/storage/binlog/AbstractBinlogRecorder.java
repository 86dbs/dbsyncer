package org.dbsyncer.storage.binlog;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.file.BufferedRandomAccessFile;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.charset.Charset;
import java.util.Queue;

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

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private static final String BINLOG = "binlog";

    private static final String BINLOG_INDEX = BINLOG + ".index";

    private static final String BINLOG_CONFIG = BINLOG + ".config";

    private static final long PERIOD = 3000;

    private String path;

    private File configPath;

    private Binlog binlog;

    private BinlogPipeline pipeline;

    private OutputStream out;

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
        if (!getQueue().isEmpty()) {
            return;
        }

        try {
            byte[] line;
            boolean hasLine = false;
            while (null != (line = pipeline.readLine())) {
                deserialize(BinlogMessage.parseFrom(line));
//                getQueue().offer(deserialize(message));
                hasLine = true;
            }

            if (hasLine) {
                binlog.setPos(pipeline.getFilePointer());
                FileUtils.writeStringToFile(configPath, JsonUtil.objToJson(binlog), DEFAULT_CHARSET);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void flush(BinlogMessage message) {
        if (null != message) {
            try {
                message.writeDelimitedTo(out);
                out.write(LINE_SEPARATOR.getBytes(DEFAULT_CHARSET));
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public void destroy() {
        IOUtils.closeQuietly(out);
        IOUtils.closeQuietly(pipeline.getRaf());
    }

    private void initPipeline() throws IOException {
        // binlog.config
        configPath = new File(path + BINLOG_CONFIG);
        if (!configPath.exists()) {
            final String binlogName = createBinlogName(1);
            binlog = new Binlog().setBinlog(binlogName);
            FileUtils.writeStringToFile(configPath, JsonUtil.objToJson(binlog), DEFAULT_CHARSET);

            // binlog.000001
            FileUtils.writeStringToFile(new File(path + binlog.getBinlog()), "", DEFAULT_CHARSET);

            // binlog.index
            FileUtils.writeStringToFile(new File(path + BINLOG_INDEX), binlogName + LINE_SEPARATOR, DEFAULT_CHARSET);
        }

        String configJson = FileUtils.readFileToString(configPath, Charset.defaultCharset());
        binlog = JsonUtil.jsonToObj(configJson, Binlog.class);
        File binlogFile = new File(path + binlog.getBinlog());
        Assert.isTrue(binlogFile.exists(), String.format("The binlogFile '%s' is not exist.", binlogFile.getAbsolutePath()));

        final RandomAccessFile raf = new BufferedRandomAccessFile(binlogFile, "r");
        raf.seek(binlog.getPos());
        pipeline = new BinlogPipeline(raf);
        out = new FileOutputStream(binlogFile, true);
    }

    private String createBinlogName(int index) {
        return String.format("%s.%06d", BINLOG, index <= 0 ? 1 : index);
    }

}
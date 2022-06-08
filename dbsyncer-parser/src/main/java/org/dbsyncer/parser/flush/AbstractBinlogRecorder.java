package org.dbsyncer.parser.flush;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.listener.file.BufferedRandomAccessFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/8 0:53
 */
public abstract class AbstractBinlogRecorder implements BinlogRecorder, ScheduledTaskJob {

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    private static final long PERIOD = 1000;

    private static final long BINLOG_MAX_SIZE = 512 * 1024 * 1024;

    private static final int BINLOG_EXPIRE_DAYS = 7;

    private static final String BINLOG = "binlog";

    private static final String BINLOG_CONFIG = BINLOG + ".config";

    private static final String BINLOG_INDEX = BINLOG + ".index";

    private static final String BINLOG_TEMP = BINLOG + ".temp";

    private String path;

    private Binlog binlog;

    private RandomAccessFile raf;

    @PostConstruct
    private void init() throws IOException {
        scheduledTaskService.start(PERIOD, this, String.format(".%s", BinlogRecorder.class.getSimpleName()));
        initPipeline(getClass().getSimpleName());
    }

    protected abstract BufferActuator getBufferActuator();

    @Override
    public void run() {
        // TODO 同步消息到缓存队列
    }

    @Override
    public void flush(BufferRequest request) {
        // TODO 序列化消息
    }

    private void initPipeline(String fileName) throws IOException {
        // /data/binlog/{BufferActuator}/
        path = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("data").append(File.separatorChar)
                .append("binlog").append(File.separatorChar)
                .append(fileName).append(File.separatorChar)
                .toString();

        File dir = new File(path);
        File binlogConfigFile = new File(path.concat(BINLOG_CONFIG));
        if (!dir.exists()) {
            FileUtils.forceMkdir(dir);
            binlog = new Binlog().setBinlog(String.format("%s.%06d", BINLOG, 1));
            FileUtils.writeStringToFile(binlogConfigFile, JsonUtil.objToJson(binlog), Charset.defaultCharset());
            return;
        }

        Assert.isTrue(binlogConfigFile.exists(), String.format("The binlogConfigFile '%s' is null.", binlogConfigFile.getAbsolutePath()));

        String binlogJsonStr = FileUtils.readFileToString(binlogConfigFile, Charset.defaultCharset());
        binlog = JsonUtil.jsonToObj(binlogJsonStr, Binlog.class);
        File binlogFile = new File(path.concat(binlog.getBinlog()));
        raf = new BufferedRandomAccessFile(binlogFile, "rw");
    }

    public static void main(String[] args) {
        System.out.println(String.format("%s.%06d", BINLOG, 1));
    }

    static class Binlog {
        private String binlog;
        private long pos = 0;

        public String getBinlog() {
            return binlog;
        }

        public Binlog setBinlog(String binlog) {
            this.binlog = binlog;
            return this;
        }

        public long getPos() {
            return pos;
        }

        public Binlog setPos(long pos) {
            this.pos = pos;
            return this;
        }
    }
}
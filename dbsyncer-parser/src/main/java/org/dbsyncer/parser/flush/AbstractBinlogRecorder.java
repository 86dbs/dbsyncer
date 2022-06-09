package org.dbsyncer.parser.flush;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.listener.file.BufferedRandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Queue;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/8 0:53
 */
public abstract class AbstractBinlogRecorder implements BinlogRecorder, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final long PERIOD = 1000;

    private static final long BINLOG_MAX_SIZE = 512 * 1024 * 1024;

    private static final int BINLOG_EXPIRE_DAYS = 7;

    private static final String BINLOG = "binlog";

    private static final String BINLOG_INDEX = BINLOG + ".index";

    private static final String BINLOG_CONFIG = BINLOG + ".config";

    private String path;

    private File configPath;

    private Binlog binlog;

    private Pipeline pipeline;

    private OutputStream writer;

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    @PostConstruct
    private void init() throws IOException {
        // /data/binlog/{BufferActuator}/
        path = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar)
                .append("data").append(File.separatorChar)
                .append("binlog").append(File.separatorChar)
                .append(getClass().getSimpleName()).append(File.separatorChar)
                .toString();
        File dir = new File(path);
        if (!dir.exists()) {
            FileUtils.forceMkdir(dir);
        }

        initPipeline();
    }

    protected abstract Queue getBuffer();

    public void run() {
        // 如果缓冲繁忙，继续等待
        if (getBuffer().size() > 500) {
            return;
        }

        try {
            String line;
            boolean hasLine = false;
            while (null != (line = pipeline.readLine())) {
                if (StringUtil.isNotBlank(line)) {
                    logger.info(line);
                    hasLine = true;
                }
            }

            if (hasLine) {
                binlog.setPos(pipeline.filePointer);
                FileUtils.writeStringToFile(configPath, JsonUtil.objToJson(binlog), DEFAULT_CHARSET);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void flush(BufferRequest request) {
        try {
            writeLine(writer, JsonUtil.objToJson(request));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void destroy() {
        IOUtils.closeQuietly(writer);
        IOUtils.closeQuietly(pipeline.raf);
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
        pipeline = new Pipeline(raf);
        writer = new FileOutputStream(binlogFile, true);
    }

    private void writeLine(final OutputStream output, final String line) throws IOException {
        if (line == null) {
            return;
        }
        if (line != null) {
            output.write(line.getBytes(DEFAULT_CHARSET));
        }
        output.write(LINE_SEPARATOR.getBytes(DEFAULT_CHARSET));
    }

    private String createBinlogName(int index) {
        return String.format("%s.%06d", BINLOG, index <= 0 ? 1 : index);
    }

    final class Pipeline {
        RandomAccessFile raf;
        byte[] b;
        long filePointer;

        public Pipeline(RandomAccessFile raf) {
            this.raf = raf;
        }

        public String readLine() throws IOException {
            this.filePointer = raf.getFilePointer();
            if (filePointer >= raf.length()) {
                b = new byte[0];
                return null;
            }
            if (b == null || b.length == 0) {
                b = new byte[(int) (raf.length() - filePointer)];
            }
            raf.read(b);

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            int read = 0;
            for (int i = 0; i < b.length; i++) {
                read++;
                if (b[i] == '\n' || b[i] == '\r') {
                    break;
                }
                stream.write(b[i]);
            }
            b = Arrays.copyOfRange(b, read, b.length);

            raf.seek(this.filePointer + read);
            byte[] _b = stream.toByteArray();
            stream.close();
            stream = null;
            return new String(_b, DEFAULT_CHARSET);
        }
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
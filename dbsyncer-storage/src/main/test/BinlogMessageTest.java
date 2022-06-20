import com.google.protobuf.ByteString;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.file.BufferedRandomAccessFile;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.storage.binlog.Binlog;
import org.dbsyncer.storage.binlog.BinlogPipeline;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.Data;
import org.dbsyncer.storage.binlog.proto.EventEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/18 23:46
 */
public class BinlogMessageTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String path;

    private OutputStream out;

    private BinlogPipeline pipeline;

    @Before
    public void init() throws IOException {
        File dir = new File(System.getProperty("user.dir")).getParentFile();
        path = new StringBuilder(dir.getAbsolutePath()).append(File.separatorChar)
                .append("data").append(File.separatorChar)
                .append("binlog").append(File.separatorChar)
                .append("WriterBinlog").append(File.separatorChar)
                .toString();
        File configPath = new File(path + "binlog.config");
        String configJson = FileUtils.readFileToString(configPath, Charset.defaultCharset());
        Binlog binlog = JsonUtil.jsonToObj(configJson, Binlog.class);
        File binlogFile = new File(path + binlog.getBinlog());
        out = new FileOutputStream(binlogFile, true);

        final RandomAccessFile raf = new BufferedRandomAccessFile(binlogFile, "r");
        raf.seek(binlog.getPos());
        pipeline = new BinlogPipeline(raf);
    }

    @After
    public void close() {
        IOUtils.closeQuietly(out);
        IOUtils.closeQuietly(pipeline.getRaf());
    }

    @Test
    public void testBinlogMessage() throws IOException {
        write("123456", "abc");
        write("000111", "xyz");
        write("888999", "jkl");

        byte[] line;
        while (null != (line = pipeline.readLine())) {
            BinlogMessage binlogMessage = BinlogMessage.parseFrom(line);
            logger.info(binlogMessage.toString());
        }
    }

    private void write(String tableGroupId, String key) throws IOException {
        BinlogMessage build = BinlogMessage.newBuilder()
                .setTableGroupId(tableGroupId)
                .setEvent(EventEnum.UPDATE)
                .addData(Data.newBuilder().putRow(key, ByteString.copyFromUtf8("hello,中国")).build())
                .build();
        byte[] bytes = build.toByteArray();
        logger.info("序列化长度：{}", bytes.length);
        logger.info("{}", bytes);
        build.writeDelimitedTo(out);
    }

}
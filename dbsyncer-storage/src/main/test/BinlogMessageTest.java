import com.google.protobuf.ByteString;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.storage.binlog.BinlogContext;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.EventEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/18 23:46
 */
public class BinlogMessageTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private BinlogContext context;

    @Before
    public void init() throws IOException {
        context = new BinlogContext("WriterBinlog");
    }

    @After
    public void close() {
        IOUtils.closeQuietly(context);
    }

    @Test
    public void testBinlogMessage() throws IOException {
        for (int i = 0; i <= 9; i++) {
            String s = String.format("%s.%06d", "BINLOG", i % 999999 + 1);
            logger.info("{} {}", i, s);
        }

        write("123456", "abc");
        write("000111", "xyz");
        write("888999", "jkl");

        byte[] line;
        while (null != (line = context.readLine())) {
            BinlogMessage binlogMessage = BinlogMessage.parseFrom(line);
            logger.info(binlogMessage.toString());
        }
    }

    private void write(String tableGroupId, String key) throws IOException {
        BinlogMessage build = BinlogMessage.newBuilder()
                .setTableGroupId(tableGroupId)
                .setEvent(EventEnum.UPDATE)
                .setData(BinlogMap.newBuilder().putRow(key, ByteString.copyFromUtf8("hello,中国")))
                .build();
        byte[] bytes = build.toByteArray();
        logger.info("序列化长度：{}", bytes.length);
        logger.info("{}", bytes);
        context.write(build);
    }

}
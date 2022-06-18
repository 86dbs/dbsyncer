import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.Data;
import org.dbsyncer.storage.binlog.proto.Event;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/18 23:46
 */
public class BinlogMessageTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testBinlogMessage() throws InvalidProtocolBufferException {
        LocalTime localTime = DateFormatUtil.stringToLocalTime("2022-06-18 22:59:59");
        String s = localTime.toString();

        BinlogMessage build = BinlogMessage.newBuilder()
                .setTableGroupId("123456788888")
                .setEvent(Event.UPDATE)
                .addData(Data.newBuilder().putRow("aaa", ByteString.copyFromUtf8("hello,中国")).putRow("aaa111", ByteString.copyFromUtf8(s)))
                .build();

        byte[] bytes = build.toByteArray();
        logger.info("序列化长度：{}", bytes.length);
        logger.info("{}", bytes);

        BinlogMessage message = BinlogMessage.parseFrom(bytes);
        logger.info(message.getTableGroupId());
        logger.info(message.getEvent().name());
        logger.info(message.getDataList().toString());
    }

}
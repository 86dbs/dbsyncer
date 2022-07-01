import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/1 23:44
 */
public class BinlogMessageFieldTypeTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testMessageFieldType() {
        final ByteBuffer buffer = ByteBuffer.allocate(32);
        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
        ByteBuffer byteBuffer = buffer.putLong(timestamp.getTime());
        byteBuffer.flip();
        byte[] bytes = new byte[8];
        byteBuffer.get(bytes);
        logger.info("remaining:{}, position:{}, limit:{}, arrayOffset:{}, bytes:{}", byteBuffer.remaining(), byteBuffer.position(), byteBuffer.limit(), byteBuffer.arrayOffset(), bytes);

        byteBuffer.clear();
        timestamp = Timestamp.valueOf(LocalDateTime.now());
        byteBuffer = buffer.putLong(timestamp.getTime());
        byteBuffer.flip();
        byteBuffer.get(bytes);
        logger.info("remaining:{}, position:{}, limit:{}, arrayOffset:{}, bytes:{}", byteBuffer.remaining(), byteBuffer.position(), byteBuffer.limit(), byteBuffer.arrayOffset(), bytes);

        byteBuffer.clear();
        timestamp = Timestamp.valueOf(LocalDateTime.now());
        byteBuffer = buffer.putLong(timestamp.getTime());
        byteBuffer.flip();
        byteBuffer.get(bytes);
        logger.info("remaining:{}, position:{}, limit:{}, arrayOffset:{}, bytes:{}", byteBuffer.remaining(), byteBuffer.position(), byteBuffer.limit(), byteBuffer.arrayOffset(), bytes);
    }

}
import com.github.shyiko.mysql.binlog.event.Event;
import org.dbsyncer.listener.mysql.BinaryLogClient;
import org.dbsyncer.listener.mysql.BinaryLogRemoteClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-11-13 22:25
 */
public class BinaryLogRemoteClientTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testConnect() throws Exception {
        String hostname = "127.0.0.1";
        int port = 3306;
        String username = "root";
        String password = "123";

        BinaryLogClient client = new BinaryLogRemoteClient(hostname, port, username, password);
        client.setBinlogFilename("mysql_bin.000029");
        client.setBinlogPosition(154);
        client.registerEventListener(new BinaryLogRemoteClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                logger.info(event.toString());
            }
        });
        client.registerLifecycleListener(new BinaryLogRemoteClient.LifecycleListener() {
            @Override
            public void onConnect(BinaryLogRemoteClient client) {
                logger.info("建立连接");
            }

            @Override
            public void onCommunicationFailure(BinaryLogRemoteClient client, Exception ex) {
                logger.error("连接异常", ex);
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogRemoteClient client, Exception ex) {
                logger.error("解析异常", ex);
            }

            @Override
            public void onDisconnect(BinaryLogRemoteClient client) {
                logger.error("断开连接");
            }
        });

        client.connect();
    }

}

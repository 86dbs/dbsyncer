/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.dbsyncer.connector.mysql.binlog.BinaryLogClient;
import org.dbsyncer.connector.mysql.binlog.BinaryLogRemoteClient;
import org.dbsyncer.sdk.constant.ConnectorConstant;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
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
        // client.setBinlogFilename("mysql_bin.000028");
        // client.setBinlogPosition(1149);
        client.registerEventListener(event-> {
            // ROTATE > FORMAT_DESCRIPTION > TABLE_MAP > WRITE_ROWS > UPDATE_ROWS > DELETE_ROWS > XID
            EventHeader header = event.getHeader();
            // XID
            if (header.getEventType() == EventType.XID) {
                logger.info(header.toString());
                return;
            }

            if (EventType.isUpdate(header.getEventType())) {
                UpdateRowsEventData data = event.getData();
                data.getRows().forEach(m-> {
                    List<Object> before = Stream.of(m.getKey()).collect(Collectors.toList());
                    List<Object> after = Stream.of(m.getValue()).collect(Collectors.toList());
                    logger.info("event:{}, tableName:{}, before:{}, after:{}", ConnectorConstant.OPERTION_UPDATE, data.getTableId(), before, after);
                });
                return;
            }
            if (EventType.isWrite(header.getEventType())) {
                WriteRowsEventData data = event.getData();
                data.getRows().forEach(m-> {
                    List<Object> after = Stream.of(m).collect(Collectors.toList());
                    logger.info("event:{}, tableName:{}, before:{}, after:{}", ConnectorConstant.OPERTION_INSERT, data.getTableId(), Collections.emptyList(), after);
                });
                return;
            }
            if (EventType.isDelete(header.getEventType())) {
                DeleteRowsEventData data = event.getData();
                data.getRows().forEach(m-> {
                    List<Object> before = Stream.of(m).collect(Collectors.toList());
                    logger.info("event:{}, tableName:{}, before:{}, after:{}", ConnectorConstant.OPERTION_DELETE, data.getTableId(), before, Collections.emptyList());
                });
                return;
            }

            if (header.getEventType() == EventType.ROTATE) {
                RotateEventData data = event.getData();
                logger.info(data.toString());
                return;
            }
        });
        client.registerLifecycleListener(new BinaryLogRemoteClient.LifecycleListener() {

            @Override
            public void onConnect(BinaryLogRemoteClient client) {
                logger.info("建立连接");
            }

            @Override
            public void onException(BinaryLogRemoteClient client, Exception ex) {
                logger.error("连接异常", ex);
            }

            @Override
            public void onDisconnect(BinaryLogRemoteClient client) {
                logger.error("断开连接");
            }
        });

        client.connect();

        logger.info("test wait...");
        // TimeUnit.SECONDS.sleep(300);
        // client.disconnect();
        Thread.currentThread().join();
        logger.info("test end");
    }

}
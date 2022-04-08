import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/8 23:06
 */
public class PGReplicationTest {

    public static void main(String[] args) throws SQLException, InterruptedException {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        Properties props = new Properties();
        PGProperty.USER.set(props, "postgres");
        PGProperty.PASSWORD.set(props, "123456");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "14.2");
        PGProperty.REPLICATION.set(props, "postgres");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        Connection con = DriverManager.getConnection(url, props);
        PGConnection replConnection = con.unwrap(PGConnection.class);
        replConnection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName("test_slot")
                .withOutputPlugin("wal2json")
                .make();
        PGReplicationStream stream =
                replConnection.getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName("test_slot")
                        .start();
        while (true) {
            //non blocking receive message
            ByteBuffer msg = stream.readPending();

            if (msg == null) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }
            int offset = msg.arrayOffset();
            byte[] source = msg.array();
            int length = source.length - offset;
            System.out.println(new String(source, offset, length));
        }

    }

}

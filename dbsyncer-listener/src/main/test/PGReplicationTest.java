import org.dbsyncer.connector.util.DatabaseUtil;
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/8 23:06
 */
public class PGReplicationTest {

    public static void main(String[] args) throws SQLException, InterruptedException {
        String url = "jdbc:postgresql://127.0.0.1:5432/postgres";
        String driverClassNam = "org.postgresql.Driver";
        String username = "postgres";
        String password = "123456";
        String slotName = "test_slot";
        Connection con = DatabaseUtil.getConnection(driverClassNam, url, username, password);
        PGConnection replConnection = con.unwrap(PGConnection.class);
        replConnection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName(slotName)
                .withOutputPlugin("wal2json")
                .make();
        PGReplicationStream stream = replConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
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

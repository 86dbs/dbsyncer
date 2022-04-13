import org.dbsyncer.connector.util.DatabaseUtil;
import org.junit.Test;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/8 23:06
 */
public class PGReplicationTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Connection connection;

    @Test
    public void testPG() throws SQLException, InterruptedException {
        String url = "jdbc:postgresql://127.0.0.1:5432/postgres";
        String driverClassNam = "org.postgresql.Driver";
        String username = "postgres";
        String password = "123456";
        connection = DatabaseUtil.getConnection(driverClassNam, url, username, password);

        LogSequenceNumber currentLSN = query("SELECT pg_current_wal_lsn()", rs -> LogSequenceNumber.valueOf(rs.getString(1)));

        PGConnection replConnection = connection.unwrap(PGConnection.class);
        PGReplicationStream stream = replConnection
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName("test_slot")
                .withStartPosition(currentLSN)
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

    public <T> T query(String sql, ResultSetMapper mapper) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        T apply = null;
        try {
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if (rs.next()) {
                apply = (T) mapper.apply(rs);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            close(rs);
            close(ps);
        }
        return apply;
    }

    private void close(AutoCloseable closeable) {
        if (null != closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    public interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

}

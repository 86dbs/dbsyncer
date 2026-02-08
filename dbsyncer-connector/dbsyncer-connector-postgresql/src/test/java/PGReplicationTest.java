import org.junit.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Properties;
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
        // String slotName = "test_slot";
        String slotName = "test_pgoutput";
        // String outputPlugin = "test_decoding";
        String outputPlugin = "pgoutput";
        String publicationName = "mypub";

        Properties props = new Properties();
        PGProperty.USER.set(props, username);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        connection = DriverManager.getConnection(url, props);

        LogSequenceNumber lsn = currentXLogLocation();

        PGConnection pgConnection = connection.unwrap(PGConnection.class);

        // pgConnection.getReplicationAPI()
        // .createReplicationSlot()
        // .logical()
        // .withSlotName(slotName)
        // .withOutputPlugin(outputPlugin)
        // .make();

        PGReplicationStream stream = pgConnection.getReplicationAPI().replicationStream().logical().withSlotName(slotName)
                // .withSlotOption("include-xids", true)
                // .withSlotOption("skip-empty-xacts", true)
                .withSlotOption("proto_version", 1).withSlotOption("publication_names", publicationName).withStatusInterval(5, TimeUnit.SECONDS).withStartPosition(lsn).start();

        try {
            Thread.sleep(10);
        } catch (Exception e) {
        }
        stream.forceUpdateStatus();
        while (true) {
            // non blocking receive message
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

    /**
     * Returns the current position in the server tx log.
     *
     * @return a long value, never negative
     * @throws SQLException if anything unexpected fails.
     */
    public LogSequenceNumber currentXLogLocation() throws SQLException {
        int majorVersion = connection.getMetaData().getDatabaseMajorVersion();
        String sql = majorVersion >= 10 ? "select * from pg_current_wal_lsn()" : "select * from pg_current_xlog_location()";
        return query(sql, rs->LogSequenceNumber.valueOf(rs.getString(1)));
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

    public boolean execute(String sql) {
        PreparedStatement ps = null;
        boolean execute = false;
        try {
            ps = connection.prepareStatement(sql);
            execute = ps.execute();
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            close(ps);
        }
        return execute;
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

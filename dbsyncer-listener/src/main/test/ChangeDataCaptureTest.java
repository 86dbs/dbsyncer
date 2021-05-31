import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-05-18 22:25
 */
public class ChangeDataCaptureTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String  GET_MAX_TRANSACTION_LSN        = "SELECT MAX(start_lsn) FROM cdc.lsn_time_mapping WHERE tran_id <> 0x00";
    private static final String  GET_LIST_OF_CDC_ENABLED_TABLES = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final Pattern BRACKET_PATTERN                = Pattern.compile("[\\[\\]]");

    private Connection conn = null;

    @Test
    public void testConnect() throws SQLException {
        ChangeDataCaptureTest cdc = new ChangeDataCaptureTest();
        cdc.start();

        logger.info("read log");
        cdc.queryAndMap(GET_LIST_OF_CDC_ENABLED_TABLES, rs -> {
            final Set<SqlServerChangeTable> changeTables = new HashSet<>();
            while (rs.next()) {
                SqlServerChangeTable changeTable = new SqlServerChangeTable(
                        // schemaName
                        rs.getString(1),
                        // tableName
                        rs.getString(2),
                        // captureInstance
                        rs.getString(3),
                        // changeTableObjectId
                        rs.getInt(4),
                        // startLsn
                        rs.getBytes(6),
                        // stopLsn
                        rs.getBytes(7),
                        // capturedColumns
                        rs.getString(15));
                changeTables.add(changeTable);
            }
            logger.info("changeTables:{} ", changeTables.size());
            return changeTables;
        });

        cdc.close();
    }

    private void start() throws SQLException {
        String username = "sa";
        String password = "Sa123456";
        String url = "jdbc:sqlserver://127.0.0.1:1434;DatabaseName=dbsyncer";
        conn = DriverManager.getConnection(url, username, password);
        if (conn != null) {
            DatabaseMetaData dm = (DatabaseMetaData) conn.getMetaData();
            System.out.println("Driver name: " + dm.getDriverName());
            System.out.println("Driver version: " + dm.getDriverVersion());
            System.out.println("Product name: " + dm.getDatabaseProductName());
            System.out.println("Product version: " + dm.getDatabaseProductVersion());
        }
    }

    private void close() {
        if (null != conn) {
            close(conn);
        }
    }

    public interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    public <T> T queryAndMap(String sql, ResultSetMapper<T> mapper) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet rs = null;
        T apply = null;
        try {
            rs = statement.executeQuery(sql);
            apply = mapper.apply(rs);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            close(rs);
            close(statement);
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

    final class SqlServerChangeTable {
        String schemaName;
        String tableName;
        String captureInstance;
        int    changeTableObjectId;
        byte[] startLsn;
        byte[] stopLsn;
        String capturedColumns;

        public SqlServerChangeTable(String schemaName, String tableName, String captureInstance,
                                    int changeTableObjectId,
                                    byte[] startLsn, byte[] stopLsn, String capturedColumns) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.captureInstance = captureInstance;
            this.capturedColumns = capturedColumns;
            this.changeTableObjectId = changeTableObjectId;
            this.startLsn = startLsn;
            this.stopLsn = stopLsn;
        }

    }

}
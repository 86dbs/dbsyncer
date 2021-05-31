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
 * @see https://www.red-gate.com/simple-talk/sql/learn-sql-server/introduction-to-change-data-capture-cdc-in-sql-server-2008/
 * @Date 2021-05-18 22:25
 */
public class ChangeDataCaptureTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String  GET_MAX_TRANSACTION_LSN        = "SELECT MAX(start_lsn) FROM cdc.lsn_time_mapping WHERE tran_id <> 0x00";
    private static final String  GET_LIST_OF_CDC_ENABLED_TABLES = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final Pattern BRACKET_PATTERN                = Pattern.compile("[\\[\\]]");

    private Connection conn = null;

    /**
     * <p>cdc.captured_columns – 此表返回捕获列列表的结果。</p>
     * <p>cdc.change_tables – 此表返回所有启用捕获的表的列表。</p>
     * <p>cdc.ddl_history – 此表包含自启用捕获数据以来所有 DDL 更改的历史记录。</p>
     * <p>cdc.index_columns – 该表包含与变更表相关的索引。</p>
     * <p>cdc.lsn_time_mapping – 此表映射 LSN编号(唯一序列号标识, 增加数字) 和时间。</p>
     * <p>cdc.fn_cdc_get_all_changes_HumanResources_Shift - 可用于获取在特定时间段内发生的事件</p>
     * <p>sys.fn_cdc_map_time_to_lsn - 表中是否有 tran_end_time值大于或等于指定时间的行。例如，可以用此查询来确定捕获进程是否已处理完截至前指定时间提交的更改</p>
     * <p>sys.fn_cdc_get_max_lsn</p>
     * <p>sys.sp_cdc_cleanup_change_table 默认情况下间隔为3天清理日志数据</p>
     *
     * @throws SQLException
     */
    @Test
    public void testConnect() throws SQLException {
        ChangeDataCaptureTest cdc = new ChangeDataCaptureTest();
        cdc.start();

        // 读取增量
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

        // 读取事务
        cdc.queryAndMap(GET_MAX_TRANSACTION_LSN, rs -> {
            while (rs.next()) {
                logger.info("[{}],[{}]", rs.getString(1), rs.getString(2));
            }
            return null;
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
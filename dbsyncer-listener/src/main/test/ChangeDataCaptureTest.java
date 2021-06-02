import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-05-18 22:25
 * @see https://www.red-gate.com/simple-talk/sql/learn-sql-server/introduction-to-change-data-capture-cdc-in-sql-server-2008/
 */
public class ChangeDataCaptureTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String GET_STATE_AGENT_SERVER = "EXEC master.dbo.xp_servicecontrol N'QUERYSTATE', N'SQLSERVERAGENT'";
    private static final String GET_DATABASE_NAME = "SELECT db_name()";
    private static final String GET_ENABLED_CDC_DATABASE = "SELECT is_cdc_enabled FROM sys.databases WHERE name = 'test'";
    private static final String GET_MAX_TRANSACTION_LSN = "SELECT MAX(start_lsn) FROM cdc.lsn_time_mapping WHERE tran_id <> 0x00";
    private static final String GET_LIST_OF_CDC_ENABLED_TABLES = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final Pattern BRACKET_PATTERN = Pattern.compile("[\\[\\]]");

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

        // 获取数据库名 test
        cdc.queryAndMap(GET_DATABASE_NAME);
        // 获取Agent服务状态 Stopped. Running.
        cdc.queryAndMap(GET_STATE_AGENT_SERVER);
        // 获取数据库CDC状态 false 0 true 1
        cdc.queryAndMap(GET_ENABLED_CDC_DATABASE);

        // 读取事务
        cdc.queryAndMap(GET_MAX_TRANSACTION_LSN);
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

        cdc.close();
    }

    private void start() throws SQLException {
        String username = "sa";
        String password = "123";
        String url = "jdbc:sqlserver://127.0.0.1:1434;DatabaseName=test";
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

    public <T> T queryAndMap(String sql) throws SQLException {
        return (T) queryAndMap(sql, rs -> {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<Map> data = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnLabel(i), rs.getObject(i));
                }
                data.add(row);
                logger.info(row.toString());
            }
            return data;
        });
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
        int changeTableObjectId;
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
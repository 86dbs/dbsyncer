import org.dbsyncer.listener.sqlserver.Lsn;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-05-18 22:25
 * @see https://www.red-gate.com/simple-talk/sql/learn-sql-server/introduction-to-change-data-capture-cdc-in-sql-server-2008/
 */
public class ChangeDataCaptureTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String GET_AGENT_SERVER_STATE = "EXEC master.dbo.xp_servicecontrol N'QUERYSTATE', N'SQLSERVERAGENT'";
    private static final String GET_DATABASE_NAME = "SELECT db_name()";
    private static final String IS_CDC_ENABLED = "SELECT is_cdc_enabled FROM sys.databases WHERE name = 'test'";

    private static final String GET_MAX_TRANSACTION_LSN = "SELECT MAX(start_lsn) FROM cdc.lsn_time_mapping WHERE tran_id <> 0x00";
    private static final String GET_MIN_LSN = "SELECT sys.fn_cdc_get_min_lsn('#')";

    private static final String LOCK_TABLE = "SELECT * FROM [#] WITH (TABLOCKX)";
    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT *# FROM cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old') order by [__$start_lsn] ASC, [__$seqval] ASC, [__$operation] ASC";
    private static final String GET_LIST_OF_CDC_ENABLED_TABLES = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final String SQL_SERVER_VERSION = "SELECT @@VERSION AS 'SQL Server Version'";
    private static final String LSN_TIMESTAMP_SELECT_STATEMENT = "sys.fn_cdc_map_lsn_to_time([__$start_lsn])";
    private static final String AT_TIME_ZONE_UTC = "AT TIME ZONE 'UTC'";

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final Pattern BRACKET_PATTERN = Pattern.compile("[\\[\\]]");

    /**
     * 数据库的实际名称，可能与连接器配置中给定的数据库名称不同
     */
    private String realDatabaseName;
    private String getAllChangesForTable;
    private String agentState;
    private boolean enabledCDC;

    private Connection connection = null;

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
        realDatabaseName = cdc.queryAndMap(GET_DATABASE_NAME, rs -> rs.getString(1));
        logger.info("数据库名:{}", realDatabaseName);

        boolean supportsAtTimeZone = supportsAtTimeZone();
        logger.info("支持时区:{}", supportsAtTimeZone);
        getAllChangesForTable = GET_ALL_CHANGES_FOR_TABLE.replaceFirst(STATEMENTS_PLACEHOLDER, Matcher.quoteReplacement(lsnTimestampSelectStatement(supportsAtTimeZone)));

        // 获取Agent服务状态 Stopped. Running.
        agentState = cdc.queryAndMap(GET_AGENT_SERVER_STATE, rs -> rs.getString(1));
        logger.info("Agent服务状态:{}", agentState);
        // 获取数据库CDC状态 false 0 true 1
        enabledCDC = cdc.queryAndMap(IS_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, realDatabaseName), rs -> rs.getBoolean(1));
        logger.info("CDC状态:{}", enabledCDC);

        // 从只读复制副本读取时，始终启用默认和唯一事务隔离是快照。这意味着CDC元数据对于长时间运行的事务不可见。
        //因此，有必要在每次读取之前重新启动事务。对于R/W数据库，执行常规提交以保持TempDB的大小是很重要的
//        connection.commit();

        // 读取LSN 00000017:0000080d:0008
        byte[] bytes = cdc.queryAndMap(GET_MAX_TRANSACTION_LSN, rs -> rs.getBytes(1));
        Lsn lsn = new Lsn(bytes);
        logger.info("最新LSN:{}", lsn);

        // 读取增量
        Set<SqlServerChangeTable> changeTables = cdc.queryAndMapList(GET_LIST_OF_CDC_ENABLED_TABLES, rs -> {
            final Set<SqlServerChangeTable> tables = new HashSet<>();
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
                tables.add(changeTable);
            }
            return tables;
        });
        logger.info("监听表数:{} ", changeTables.size());
        changeTables.forEach(t -> logger.info(t.toString()));

        // Terminate the transaction otherwise CDC could not be disabled for tables
//        connection.rollback();

        cdc.close();
    }

    private void start() throws SQLException {
        String username = "sa";
        String password = "123";
        String url = "jdbc:sqlserver://127.0.0.1:1434;DatabaseName=test";
        connection = DriverManager.getConnection(url, username, password);
        if (connection != null) {
            DatabaseMetaData dm = (DatabaseMetaData) connection.getMetaData();
            System.out.println("Driver name: " + dm.getDriverName());
            System.out.println("Driver version: " + dm.getDriverVersion());
            System.out.println("Product name: " + dm.getDatabaseProductName());
            System.out.println("Product version: " + dm.getDatabaseProductVersion());
        }
    }

    private void close() {
        if (null != connection) {
            close(connection);
        }
    }

    public interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    public <T> T queryAndMap(String sql, ResultSetMapper<T> mapper) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = null;
        T apply = null;
        try {
            rs = statement.executeQuery(sql);
            if (rs.next()) {
                apply = mapper.apply(rs);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            close(rs);
            close(statement);
        }
        return apply;
    }

    public <T> T queryAndMapList(String sql, ResultSetMapper<T> mapper) throws SQLException {
        Statement statement = connection.createStatement();
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

    /**
     * Returns the query for obtaining the LSN-to-TIMESTAMP query. On SQL Server
     * 2016 and newer, the query will normalize the value to UTC. This means that
     * the SERVER_TIMEZONE is not necessary to be given. The returned TIMESTAMP will
     * be adjusted by the JDBC driver using this VM's TZ (as required by the JDBC
     * spec), and that same TZ will be applied when converting
     * the TIMESTAMP value into an {@code Instant}.
     */
    private String lsnTimestampSelectStatement(boolean supportsAtTimeZone) {
        String result = ", " + LSN_TIMESTAMP_SELECT_STATEMENT;
        if (supportsAtTimeZone) {
            result += " " + AT_TIME_ZONE_UTC;
        }
        return result;
    }

    /**
     * SELECT ... AT TIME ZONE only works on SQL Server 2016 and newer.
     */
    private boolean supportsAtTimeZone() {
        try {
            // Always expect the support if database is not standalone SQL Server, e.g. Azure
            return getSqlServerVersion().orElse(Integer.MAX_VALUE) > 2016;
        } catch (Exception e) {
            logger.error("Couldn't obtain database server version; assuming 'AT TIME ZONE' is not supported.", e);
            return false;
        }
    }

    private Optional<Integer> getSqlServerVersion() {
        try {
            // As per https://www.mssqltips.com/sqlservertip/1140/how-to-tell-what-sql-server-version-you-are-running/
            // Always beginning with 'Microsoft SQL Server NNNN' but only in case SQL Server is standalone
            String version = queryAndMap(SQL_SERVER_VERSION, rs -> rs.getString(1));
            if (!version.startsWith("Microsoft SQL Server ")) {
                return Optional.empty();
            }
            return Optional.of(Integer.valueOf(version.substring(21, 25)));
        } catch (Exception e) {
            throw new RuntimeException("Couldn't obtain database server version", e);
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

        @Override
        public String toString() {
            return "SqlServerChangeTable{" +
                    "schemaName='" + schemaName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", captureInstance='" + captureInstance + '\'' +
                    ", changeTableObjectId=" + changeTableObjectId +
                    ", startLsn=" + Arrays.toString(startLsn) +
                    ", stopLsn=" + Arrays.toString(stopLsn) +
                    ", capturedColumns='" + capturedColumns + '\'' +
                    '}';
        }
    }

}
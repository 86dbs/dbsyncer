import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.listener.sqlserver.Lsn;
import org.dbsyncer.listener.sqlserver.SqlServerChangeTable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-06-14 01:55
 * @Link https://www.red-gate.com/simple-talk/sql/learn-sql-server/introduction-to-change-data-capture-cdc-in-sql-server-2008/
 */
public class ChangeDataCaptureTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String GET_DATABASE_NAME = "SELECT db_name()";
    private static final String GET_DATABASE_VERSION = "SELECT @@VERSION AS 'SQL Server Version'";
    private static final String GET_TABLE_LIST = "SELECT NAME FROM SYS.TABLES WHERE SCHEMA_ID = SCHEMA_ID('dbo') AND IS_MS_SHIPPED = 0";
    private static final String IS_SERVER_AGENT_RUNNING = "EXEC master.dbo.xp_servicecontrol N'QUERYSTATE', N'SQLSERVERAGENT'";
    private static final String IS_DB_CDC_ENABLED = "SELECT is_cdc_enabled FROM sys.databases WHERE name = '#'";
    private static final String IS_TABLE_CDC_ENABLED = "SELECT COUNT(*) FROM sys.tables tb WHERE tb.is_tracked_by_cdc = 1 AND tb.name='#'";
    private static final String ENABLE_DB_CDC = "IF EXISTS(select 1 from sys.databases where name = '#' AND is_cdc_enabled=0) EXEC sys.sp_cdc_enable_db";
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from sys.tables where name = '#' AND is_tracked_by_cdc=0) EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";
    private static final String DISABLE_TABLE_CDC = "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'#', @capture_instance = 'all'";

    private static final String AT_TIME_ZONE_UTC = " AT TIME ZONE 'UTC'";
    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT sys.fn_cdc_map_lsn_to_time([__$start_lsn])#, * FROM cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old') order by [__$start_lsn] ASC, [__$seqval] ASC, [__$operation] ASC";
    private static final String GET_TABLES_CDC_ENABLED = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final String GET_MAX_LSN = "SELECT sys.fn_cdc_get_max_lsn()";
    private static final String GET_MIN_LSN = "SELECT sys.fn_cdc_get_min_lsn('#')";
    private static final String GET_INCREMENT_LSN = "SELECT sys.fn_cdc_increment_lsn(?)";

    private String realDatabaseName;
    private String getAllChangesForTable;
    private Connection connection = null;
    private Set<String> tables;

    /**
     * <p>cdc.captured_columns – 此表返回捕获列列表的结果。</p>
     * <p>cdc.change_tables – 此表返回所有启用捕获的表的列表。</p>
     * <p>cdc.ddl_history – 此表包含自启用捕获数据以来所有 DDL 更改的历史记录。</p>
     * <p>cdc.index_columns – 该表包含与变更表相关的索引。</p>
     * <p>cdc.lsn_time_mapping – 此表映射 LSN编号(唯一序列号标识, 增加数字) 和时间。</p>
     * <p>cdc.fn_cdc_get_all_changes_MY_USER - 可用于获取在特定时间段内发生的事件</p>
     * <p>sys.fn_cdc_map_time_to_lsn - 表中是否有 tran_end_time值大于或等于指定时间的行。例如，可以用此查询来确定捕获进程是否已处理完截至前指定时间提交的更改</p>
     * <p>sys.fn_cdc_get_max_lsn</p>
     * <p>sys.sp_cdc_cleanup_change_table 默认情况下间隔为3天清理日志数据</p>
     *
     * @throws SQLException
     */
    @Test
    public void testConnect() throws SQLException, InterruptedException {
        ChangeDataCaptureTest cdc = new ChangeDataCaptureTest();
        cdc.start();

        // 获取数据库名 test
        realDatabaseName = cdc.queryAndMap(GET_DATABASE_NAME, rs -> rs.getString(1));
        logger.info("数据库名:{}", realDatabaseName);
        // As per https://www.mssqltips.com/sqlservertip/1140/how-to-tell-what-sql-server-version-you-are-running/
        // Always beginning with 'Microsoft SQL Server NNNN' but only in case SQL Server is standalone
        String version = cdc.queryAndMap(GET_DATABASE_VERSION, rs -> rs.getString(1));
        boolean supportsAtTimeZone = false;
        if (version.startsWith("Microsoft SQL Server ")) {
            supportsAtTimeZone = 2016 < Integer.valueOf(version.substring(21, 25));
        }
        logger.info("数据库版本:{}", version);
        tables = cdc.queryAndMapList(GET_TABLE_LIST, rs -> {
            Set<String> table = new LinkedHashSet<>();
            while (rs.next()) {
                table.add(rs.getString(1));
            }
            return table;
        });
        logger.info("所有表:{}", tables);
        // 获取Agent服务状态 Stopped. Running.
        boolean enabledServerAgent = cdc.queryAndMap(IS_SERVER_AGENT_RUNNING, rs -> "Running.".equals(rs.getString(1)));
        logger.info("是否启动Agent服务:{}", enabledServerAgent);
        Assert.assertTrue("The agent server is not running", enabledServerAgent);
        boolean enabledCDC = cdc.queryAndMap(IS_DB_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, realDatabaseName), rs -> rs.getBoolean(1));
        logger.info("是否启用CDC库[{}]:{}", realDatabaseName, enabledCDC);
        if (!enabledCDC) {
            cdc.execute(ENABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, realDatabaseName));
            // make sure DB has cdc-enabled before proceeding
            TimeUnit.SECONDS.sleep(3);
        }

        // 注册CDC表
        tables.forEach(table -> {
            try {
                boolean enabledTableCDC = cdc.queryAndMap(IS_TABLE_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, table), rs -> rs.getInt(1) > 0);
                logger.info("是否启用CDC表[{}]:{}", table, enabledTableCDC);
                if (!enabledTableCDC) {
                    cdc.execute(ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, table));
                    Lsn minLsn = cdc.queryAndMap(GET_MIN_LSN.replace(STATEMENTS_PLACEHOLDER, table), rs -> new Lsn(rs.getBytes(1)));
                    logger.info("启用CDC表[{}]:{}", table, minLsn.isAvailable());
                }
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        });

        // 支持UTC
        getAllChangesForTable = GET_ALL_CHANGES_FOR_TABLE.replaceFirst(STATEMENTS_PLACEHOLDER, Matcher.quoteReplacement(supportsAtTimeZone ? AT_TIME_ZONE_UTC : ""));

        // 读取增量
        Set<SqlServerChangeTable> changeTables = cdc.queryAndMapList(GET_TABLES_CDC_ENABLED, rs -> {
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
                logger.info(changeTable.toString());
                tables.add(changeTable);
            }
            return tables;
        });
        logger.info("监听表数:{} ", changeTables.size());

        if (!CollectionUtils.isEmpty(changeTables)) {
            AtomicInteger count = new AtomicInteger(0);
            Lsn lastLsn = cdc.queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));

            while (true && count.getAndAdd(1) < 30) {
                Lsn stopLsn = cdc.queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));
                if (!stopLsn.isAvailable()) {
                    logger.warn("No maximum LSN recorded in the database; please ensure that the SQL Server Agent is running");
                    cdc.pause();
                    continue;
                }

                if (stopLsn.compareTo(lastLsn) <= 0) {
                    cdc.pause();
                    continue;
                }

                Lsn startLsn = getIncrementLsn(cdc, lastLsn);
                changeTables.forEach(changeTable -> {
                    try {
                        final String query = getAllChangesForTable.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
                        logger.info("Getting changes for table {} in range[{}, {}]", changeTable.getTableName(), startLsn, stopLsn);

                        cdc.queryAndMapList(query, statement -> {
                            statement.setBytes(1, startLsn.getBinary());
                            statement.setBytes(2, stopLsn.getBinary());
                        }, rs -> {
                            int columnCount = rs.getMetaData().getColumnCount();
                            List<List<Object>> data = new ArrayList<>(columnCount);
                            List<Object> row = null;
                            while (rs.next()) {
                                row = new ArrayList<>(columnCount);
                                for (int i = 1; i <= columnCount; i++) {
                                    row.add(rs.getObject(i));
                                }
                                logger.info("rows:{}", row);
                                data.add(row);
                            }
                            return data;
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                lastLsn = stopLsn;
            }
        }

        // 注销CDC表
        for (String table : tables) {
            cdc.execute(DISABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, table));
        }
        cdc.close();
    }

    public void start() throws SQLException {
        String username = "sa";
        String password = "123";
        String url = "jdbc:sqlserver://127.0.0.1:1434;DatabaseName=test";
        connection = DriverManager.getConnection(url, username, password);
        if (connection != null) {
            DatabaseMetaData dm = (DatabaseMetaData) connection.getMetaData();
            logger.info("Driver name: " + dm.getDriverName());
            logger.info("Driver version: " + dm.getDriverVersion());
            logger.info("Product name: " + dm.getDatabaseProductName());
            logger.info("Product version: " + dm.getDatabaseProductVersion());
        }
    }

    public void close() {
        if (null != connection) {
            close(connection);
        }
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

    private Lsn getIncrementLsn(ChangeDataCaptureTest cdc, Lsn lastLsn) {
        return cdc.queryAndMap(GET_INCREMENT_LSN, statement -> statement.setBytes(1, lastLsn.getBinary()), rs -> Lsn.valueOf(rs.getBytes(1)));
    }

    private void pause() throws InterruptedException {
        TimeUnit.SECONDS.sleep(2);
    }

    private void execute(String... sqlStatements) throws SQLException {
        Statement statement = connection.createStatement();
        try {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) {
                    logger.info("executing '{}'", sqlStatement);
                    statement.execute(sqlStatement);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            close(statement);
        }
    }

    public interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    public interface StatementPreparer {
        void accept(PreparedStatement statement) throws SQLException;
    }

    public <T> T queryAndMap(String sql, ResultSetMapper<T> mapper) {
        return queryAndMap(sql, null, mapper);
    }

    public <T> T queryAndMap(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        T apply = null;
        try {
            ps = connection.prepareStatement(sql);
            if (null != statementPreparer) {
                statementPreparer.accept(ps);
            }
            rs = ps.executeQuery();
            if (rs.next()) {
                apply = mapper.apply(rs);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            close(rs);
            close(ps);
        }
        return apply;
    }

    public <T> T queryAndMapList(String sql, ResultSetMapper<T> mapper) {
        return queryAndMapList(sql, null, mapper);
    }

    public <T> T queryAndMapList(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        T apply = null;
        try {
            ps = connection.prepareStatement(sql);
            if (null != statementPreparer) {
                statementPreparer.accept(ps);
            }
            rs = ps.executeQuery();
            apply = mapper.apply(rs);
        } catch (SQLServerException e) {
            // 为过程或函数 cdc.fn_cdc_get_all_changes_ ...  提供的参数数目不足。
            logger.warn(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            close(rs);
            close(ps);
        }
        return apply;
    }

}
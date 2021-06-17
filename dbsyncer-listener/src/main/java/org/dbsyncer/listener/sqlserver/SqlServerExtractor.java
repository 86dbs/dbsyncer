package org.dbsyncer.listener.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.commons.lang.math.RandomUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-06-18 01:20
 */
public class SqlServerExtractor extends AbstractExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String GET_DATABASE_NAME = "SELECT db_name()";
    private static final String GET_DATABASE_VERSION = "SELECT @@VERSION AS 'SQL Server Version'";
    private static final String GET_TABLE_LIST = "SELECT NAME FROM SYS.TABLES WHERE SCHEMA_ID = SCHEMA_ID('DBO') AND IS_MS_SHIPPED = 0";
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

    private static String getAllChangesForTable;
    private static Set<String> tables;
    private static Set<SqlServerChangeTable> changeTables;
    private Connection connection;
    private Worker worker;
    private Lsn lastLsn;

    @Override
    public void start() {
        try {
            connection = connect();
            tables = readTables();
            Assert.isTrue(!CollectionUtils.isEmpty(tables), "No tables available");

            boolean enabledServerAgent = queryAndMap(IS_SERVER_AGENT_RUNNING, rs -> "Running.".equals(rs.getString(1)));
            Assert.isTrue(enabledServerAgent, "Please ensure that the SQL Server Agent is running");

            enableDBCDC();
            enableTableCDC();
            changeTables = readChangeTables();
            getAllChangesForTable = getAllChangesForTableSql();

            if (null == lastLsn) {
                lastLsn = queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));
            }

            // 启动消费线程
            worker = new Worker();
            // FIXME 解析host名称
            worker.setName(new StringBuilder("cdc-parser-").append("127.0.0.1").append(":").append("1443").append("_").append(RandomUtils.nextInt(100)).toString());
            worker.setDaemon(false);
            worker.start();

        } catch (Exception e) {
            close();
            logger.error("启动失败:{}", e.getMessage());
            throw new ListenerException(e);
        }
    }

    @Override
    public void close() {
        if (null != worker && !worker.isInterrupted()) {
            worker.interrupt();
            worker = null;
        }
        disableTableCDC();
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

    private Connection connect() throws SQLException {
        final DatabaseConfig config = (DatabaseConfig) connectorConfig;
        String username = config.getUsername();
        String password = config.getPassword();
        String url = config.getUrl();
        return DriverManager.getConnection(url, username, password);
    }

    private Lsn getIncrementLsn(Lsn lastLsn) {
        return queryAndMap(GET_INCREMENT_LSN, statement -> statement.setBytes(1, lastLsn.getBinary()), rs -> Lsn.valueOf(rs.getBytes(1)));
    }

    private String getAllChangesForTableSql() {
        // 2016以上版本支持UTC
        String version = queryAndMap(GET_DATABASE_VERSION, rs -> rs.getString(1));
        boolean supportsAtTimeZone = false;
        if (version.startsWith("Microsoft SQL Server ")) {
            supportsAtTimeZone = 2016 < Integer.valueOf(version.substring(21, 25));
        }
        return GET_ALL_CHANGES_FOR_TABLE.replaceFirst(STATEMENTS_PLACEHOLDER, Matcher.quoteReplacement(supportsAtTimeZone ? AT_TIME_ZONE_UTC : ""));
    }

    private Set<String> readTables() {
        return queryAndMapList(GET_TABLE_LIST, rs -> {
            Set<String> table = new LinkedHashSet<>();
            while (rs.next()) {
                table.add(rs.getString(1));
            }
            return table;
        });
    }

    private Set<SqlServerChangeTable> readChangeTables() {
        return queryAndMapList(GET_TABLES_CDC_ENABLED, rs -> {
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
    }

    private void disableTableCDC() {
        if (CollectionUtils.isEmpty(tables)) {
            tables.forEach(table -> execute(DISABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, table)));
        }
    }

    private void enableTableCDC() {
        tables.forEach(table -> {
            boolean enabledTableCDC = queryAndMap(IS_TABLE_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, table), rs -> rs.getInt(1) > 0);
            logger.info("是否启用CDC表[{}]:{}", table, enabledTableCDC);
            if (!enabledTableCDC) {
                execute(ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, table));
                Lsn minLsn = queryAndMap(GET_MIN_LSN.replace(STATEMENTS_PLACEHOLDER, table), rs -> new Lsn(rs.getBytes(1)));
                logger.info("启用CDC表[{}]:{}", table, minLsn.isAvailable());
            }
        });
    }

    private void enableDBCDC() throws InterruptedException {
        String realDatabaseName = queryAndMap(GET_DATABASE_NAME, rs -> rs.getString(1));
        boolean enabledCDC = queryAndMap(IS_DB_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, realDatabaseName), rs -> rs.getBoolean(1));
        if (!enabledCDC) {
            execute(ENABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, realDatabaseName));
            // make sure it works
            TimeUnit.SECONDS.sleep(3);

            enabledCDC = queryAndMap(IS_DB_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, realDatabaseName), rs -> rs.getBoolean(1));
            Assert.isTrue(enabledCDC, "Please ensure that the SQL Server Agent is running");
        }
    }

    private void execute(String... sqlStatements) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
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

    private interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    private interface StatementPreparer {
        void accept(PreparedStatement statement) throws SQLException;
    }

    private <T> T queryAndMap(String sql, ResultSetMapper<T> mapper) {
        return queryAndMap(sql, null, mapper);
    }

    private <T> T queryAndMap(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
        return query(sql, statementPreparer, (rs) -> {
            rs.next();
            return mapper.apply(rs);
        });
    }

    private <T> T queryAndMapList(String sql, ResultSetMapper<T> mapper) {
        return queryAndMapList(sql, null, mapper);
    }

    private <T> T queryAndMapList(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
        return query(sql, statementPreparer, mapper);
    }

    private <T> T query(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
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
            logger.warn(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            close(rs);
            close(ps);
        }
        return apply;
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    Lsn stopLsn = queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));
                    if (!stopLsn.isAvailable()) {
                        TimeUnit.SECONDS.sleep(1);
                        continue;
                    }

                    if (stopLsn.compareTo(lastLsn) <= 0) {
                        TimeUnit.MICROSECONDS.sleep(200);
                        continue;
                    }

                    Lsn startLsn = getIncrementLsn(lastLsn);
                    changeTables.forEach(changeTable -> {
                        final String query = getAllChangesForTable.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
                        queryAndMapList(query, statement -> {
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
                                // FIXME 解析增量数据
                                data.add(row);
                            }
                            return data;
                        });
                    });

                    lastLsn = stopLsn;
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

    }
}
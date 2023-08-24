package org.dbsyncer.listener.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.dbsyncer.common.event.ChangedOffset;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.listener.AbstractDatabaseExtractor;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.enums.TableOperationEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-06-18 01:20
 */
public class SqlServerExtractor extends AbstractDatabaseExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String GET_DATABASE_NAME = "select db_name()";
    private static final String GET_TABLE_LIST = "select name from sys.tables where schema_id = schema_id('#') and is_ms_shipped = 0";
    private static final String IS_DB_CDC_ENABLED = "select is_cdc_enabled from sys.databases where name = '#'";
    private static final String IS_TABLE_CDC_ENABLED = "select count(*) from sys.tables tb where tb.is_tracked_by_cdc = 1 and tb.name='#'";
    private static final String ENABLE_DB_CDC = "IF EXISTS(select 1 from sys.databases where name = '#' and is_cdc_enabled=0) EXEC sys.sp_cdc_enable_db";
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from sys.tables where name = '#' and is_tracked_by_cdc=0) EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";
    private static final String GET_TABLES_CDC_ENABLED = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final String GET_MAX_LSN = "select sys.fn_cdc_get_max_lsn()";
    private static final String GET_MIN_LSN = "select sys.fn_cdc_get_min_lsn('#')";
    private static final String GET_INCREMENT_LSN = "select sys.fn_cdc_increment_lsn(?)";
    /**
     * https://learn.microsoft.com/zh-cn/previous-versions/sql/sql-server-2008/bb510627(v=sql.100)?redirectedfrom=MSDN
     */
    private static final String GET_ALL_CHANGES_FOR_TABLE = "select * from cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old') order by [__$start_lsn] ASC, [__$seqval] ASC";

    private static final String LSN_POSITION = "position";
    private static final int OFFSET_COLUMNS = 4;
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private Set<String> tables;
    private Set<SqlServerChangeTable> changeTables;
    private DatabaseConnectorMapper connectorMapper;
    private Worker worker;
    private Lsn lastLsn;
    private String serverName;
    private String schema;
    private final int BUFFER_CAPACITY = 256;
    private BlockingQueue<Lsn> buffer = new LinkedBlockingQueue<>(BUFFER_CAPACITY);
    private Lock lock = new ReentrantLock(true);
    private Condition isFull = lock.newCondition();
    private final Duration pollInterval = Duration.of(500, ChronoUnit.MILLIS);

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("SqlServerExtractor is already started");
                return;
            }
            connected = true;
            connect();
            readTables();
            Assert.notEmpty(tables, "No tables available");

            enableDBCDC();
            enableTableCDC();
            readChangeTables();
            readLastLsn();

            worker = new Worker();
            worker.setName(new StringBuilder("cdc-parser-").append(serverName).append("_").append(worker.hashCode()).toString());
            worker.setDaemon(false);
            worker.start();
            LsnPuller.addExtractor(metaId, this);
        } catch (Exception e) {
            close();
            logger.error("启动失败:{}", e.getMessage());
            throw new ListenerException(e);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close() {
        if (connected) {
            LsnPuller.removeExtractor(metaId);
            if (null != worker && !worker.isInterrupted()) {
                worker.interrupt();
                worker = null;
            }
            connected = false;
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (offset.getPosition() != null) {
            snapshot.put(LSN_POSITION, offset.getPosition().toString());
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

    private void connect() {
        if (connectorFactory.isAlive(connectorConfig)) {
            connectorMapper = (DatabaseConnectorMapper) connectorFactory.connect(connectorConfig);
            DatabaseConfig cfg = connectorMapper.getConfig();
            serverName = cfg.getUrl();
            schema = cfg.getSchema();
        }
    }

    private void readLastLsn() {
        if (!snapshot.containsKey(LSN_POSITION)) {
            lastLsn = queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));
            if (null != lastLsn && lastLsn.isAvailable()) {
                snapshot.put(LSN_POSITION, lastLsn.toString());
                super.forceFlushEvent();
                return;
            }
            // Shouldn't happen if the agent is running, but it is better to guard against such situation
            throw new ListenerException("No maximum LSN recorded in the database");
        }
        lastLsn = Lsn.valueOf(snapshot.get(LSN_POSITION));
    }

    private void readTables() {
        tables = queryAndMapList(GET_TABLE_LIST.replace(STATEMENTS_PLACEHOLDER, schema), rs -> {
            Set<String> table = new LinkedHashSet<>();
            while (rs.next()) {
                if (filterTable.contains(rs.getString(1))) {
                    table.add(rs.getString(1));
                }
            }
            return table;
        });
    }

    private void readChangeTables() {
        changeTables = queryAndMapList(GET_TABLES_CDC_ENABLED, rs -> {
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
    }

    private void enableTableCDC() {
        if (!CollectionUtils.isEmpty(tables)) {
            tables.forEach(table -> {
                boolean enabledTableCDC = queryAndMap(IS_TABLE_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, table), rs -> rs.getInt(1) > 0);
                if (!enabledTableCDC) {
                    execute(String.format(ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, table), schema));
                    Lsn minLsn = queryAndMap(GET_MIN_LSN.replace(STATEMENTS_PLACEHOLDER, table), rs -> new Lsn(rs.getBytes(1)));
                    logger.info("启用CDC表[{}]:{}", table, minLsn.isAvailable());
                }
            });
        }
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
        connectorMapper.execute(databaseTemplate -> {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) {
                    logger.info("executing '{}'", sqlStatement);
                    databaseTemplate.execute(sqlStatement);
                }
            }
            return true;
        });
    }

    private void pull(Lsn stopLsn) {
        Lsn startLsn = queryAndMap(GET_INCREMENT_LSN, statement -> statement.setBytes(1, lastLsn.getBinary()), rs -> Lsn.valueOf(rs.getBytes(1)));
        changeTables.forEach(changeTable -> {
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            List<CDCEvent> list = queryAndMapList(query, statement -> {
                statement.setBytes(1, startLsn.getBinary());
                statement.setBytes(2, stopLsn.getBinary());
            }, rs -> {
                int columnCount = rs.getMetaData().getColumnCount();
                List<Object> row = null;
                List<CDCEvent> data = new ArrayList<>();
                while (rs.next()) {
                    // skip update before
                    final int operation = rs.getInt(3);
                    if (TableOperationEnum.isUpdateBefore(operation)) {
                        continue;
                    }
                    row = new ArrayList<>(columnCount - OFFSET_COLUMNS);
                    for (int i = OFFSET_COLUMNS + 1; i <= columnCount; i++) {
                        row.add(rs.getObject(i));
                    }
                    data.add(new CDCEvent(changeTable.getTableName(), operation, row));
                }
                return data;
            });

            if (!CollectionUtils.isEmpty(list)) {
                parseEvent(list, stopLsn);
            }
        });
    }

    private void parseEvent(List<CDCEvent> list, Lsn stopLsn) {
        int size = list.size();
        for (int i = 0; i < size; i++) {
            boolean isEnd = i == size - 1;
            CDCEvent event = list.get(i);
            if (TableOperationEnum.isUpdateAfter(event.getCode())) {
                sendChangedEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_UPDATE, event.getRow(), null, (isEnd ? stopLsn : null)));
                continue;
            }

            if (TableOperationEnum.isInsert(event.getCode())) {
                sendChangedEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_INSERT, event.getRow(), null, (isEnd ? stopLsn : null)));
                continue;
            }

            if (TableOperationEnum.isDelete(event.getCode())) {
                sendChangedEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_DELETE, event.getRow(), null, (isEnd ? stopLsn : null)));
            }
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

    private <T> T query(String preparedQuerySql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
        Object execute = connectorMapper.execute(databaseTemplate -> {
            PreparedStatement ps = null;
            ResultSet rs = null;
            T apply = null;
            try {
                ps = databaseTemplate.getSimpleConnection().prepareStatement(preparedQuerySql);
                if (null != statementPreparer) {
                    statementPreparer.accept(ps);
                }
                rs = ps.executeQuery();
                apply = mapper.apply(rs);
            } catch (SQLServerException e) {
                // 为过程或函数 cdc.fn_cdc_get_all_changes_ ...  提供的参数数目不足。
            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                close(rs);
                close(ps);
            }
            return apply;
        });
        return (T) execute;
    }

    public Lsn getMaxLsn() {
        return queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    Lsn stopLsn = buffer.take();
                    Lsn poll;
                    while ((poll = buffer.poll()) != null) {
                        stopLsn = poll;
                    }
                    if (!stopLsn.isAvailable() || stopLsn.compareTo(lastLsn) <= 0) {
                        continue;
                    }

                    pull(stopLsn);

                    lastLsn = stopLsn;
                    snapshot.put(LSN_POSITION, lastLsn.toString());
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    if (connected) {
                        logger.error(e.getMessage(), e);
                        sleepInMills(1000L);
                    }
                }
            }
        }
    }

    public Lsn getLastLsn() {
        return lastLsn;
    }

    public void pushStopLsn(Lsn stopLsn) {
        if (buffer.contains(stopLsn)) {
            return;
        }
        if (!buffer.offer(stopLsn)) {
            try {
                lock.lock();
                while (!buffer.offer(stopLsn) && connected) {
                    logger.warn("[{}]缓存队列容量已达上限[{}], 正在阻塞重试.", this.getClass().getSimpleName(), BUFFER_CAPACITY);
                    try {
                        this.isFull.await(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
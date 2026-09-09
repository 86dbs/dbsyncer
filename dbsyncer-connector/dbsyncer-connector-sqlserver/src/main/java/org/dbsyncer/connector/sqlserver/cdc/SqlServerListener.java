/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.cdc;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.connector.sqlserver.enums.TableOperationEnum;
import org.dbsyncer.connector.sqlserver.model.CDCEvent;
import org.dbsyncer.connector.sqlserver.model.SqlServerChangeTable;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public class SqlServerListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String GET_DATABASE_NAME = "select db_name()";
    private static final String GET_TABLE_LIST = "select name from sys.tables where schema_id = schema_id('#') and is_ms_shipped = 0";
    private static final String IS_DB_CDC_ENABLED = "select is_cdc_enabled from sys.databases where name = '#'";
    private static final String IS_TABLE_CDC_ENABLED = "select count(*) from sys.tables tb where tb.is_tracked_by_cdc = 1 and tb.name='#'";
    private static final String ENABLE_DB_CDC = "IF EXISTS(select 1 from sys.databases where name = '#' and is_cdc_enabled=0) EXEC sys.sp_cdc_enable_db";
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from sys.tables where name = '#' and is_tracked_by_cdc=0) EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";
    private static final String GET_TABLES_CDC_ENABLED = "EXEC sys.sp_cdc_help_change_data_capture";
    //二进制转换出来 SELECT CONVERT(varchar(50), sys.fn_cdc_get_max_lsn(), 1) AS max_lsn;
    private static final String GET_MAX_LSN = "select sys.fn_cdc_get_max_lsn()";
    private static final String GET_MIN_LSN = "select sys.fn_cdc_get_min_lsn('#')";
    private static final String GET_INCREMENT_LSN = "select sys.fn_cdc_increment_lsn(?)";
    /**
     * https://learn.microsoft.com/zh-cn/previous-versions/sql/sql-server-2008/bb510627(v=sql.100)?redirectedfrom=MSDN
     */
    private static final String GET_ALL_CHANGES_FOR_TABLE = "select * from cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old') order by [__$start_lsn] ASC, [__$seqval] ASC";
    /**
     * 从LSN时间映射表中取出最多N个事务的提交位点，作为单批拉取的结束位点。
     * <p>提交位点是事务边界，按此切批不会拆散同一事务的变更。
     * <p>binary类型不支持max聚合，因此用嵌套TOP取最大值。
     */
    private static final String GET_BATCH_STOP_LSN = "select top (1) start_lsn from (select top (?) start_lsn from cdc.lsn_time_mapping where start_lsn > ? and start_lsn <= ? order by start_lsn asc) t order by start_lsn desc";

    private static final String LSN_POSITION = "position";
    private static final int OFFSET_COLUMNS = 4;
    /**
     * SQL Server CDC TVF在LSN为空/越界时会抛出Msg 313（文案具有误导性，实际表示区间无效）。
     * @see <a href="https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql">fn_cdc_get_all_changes</a>
     */
    private static final int CDC_INVALID_LSN_ERROR = 313;
    /**
     * 拉取变更数据时的游标大小，避免驱动一次性缓存全部结果集
     */
    private static final int FETCH_SIZE = 1000;
    /**
     * 单批拉取的事务数范围，用于把积压变更拆分成小批次
     */
    private static final int MIN_LSN_RANGE = 500;
    private static final int MAX_LSN_RANGE = 20000;
    private static final int DEFAULT_LSN_RANGE = 5000;
    /**
     * 单批拉取耗时超过该值判定为过重，需要缩小批次范围
     */
    private static final long SLOW_PULL_MILLIS = 30000;
    /**
     * 单批拉取耗时低于该值且行数不多，判定为过轻，可以扩大批次范围
     */
    private static final long FAST_PULL_MILLIS = 3000;
    /**
     * 单批拉取行数超过该值判定为过重，需要缩小批次范围
     */
    private static final long HIGH_PULL_ROWS = 100000;
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private Set<String> tables;
    private Set<SqlServerChangeTable> changeTables;
    private DatabaseConnectorInstance instance;
    private Worker worker;
    private volatile Lsn lastLsn;
    private volatile int currentLsnRange = DEFAULT_LSN_RANGE;
    private String serverName;
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
            worker.setName("cdc-parser-" + serverName + "_" + worker.hashCode());
            worker.setDaemon(false);
            worker.start();
            LsnPuller.addExtractor(metaId, this);
        } catch (Exception e) {
            close();
            logger.error("启动失败:{}", e.getMessage());
            throw new SqlServerException(e);
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
    public Map<String, String> captureSnapshot() {
        try {
            connect();
            Lsn lsn = queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));
            if (lsn == null || !lsn.isAvailable()) {
                return Collections.emptyMap();
            }
            snapshot.put(LSN_POSITION, lsn.toString());
            Map<String, String> captured = new HashMap<>(1);
            captured.put(LSN_POSITION, lsn.toString());
            return captured;
        } catch (Exception e) {
            logger.error("捕获SqlServer LSN位点失败:{}", e.getMessage(), e);
            return Collections.emptyMap();
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
        instance = (DatabaseConnectorInstance) connectorInstance;
        AbstractDatabaseConnector service = (AbstractDatabaseConnector) connectorService;
        if (service.isAlive(instance)) {
            DatabaseConfig cfg = instance.getConfig();
            serverName = cfg.getUrl();
        }
    }

    private void readLastLsn() {
        if (!snapshot.containsKey(LSN_POSITION)) {
            lastLsn = queryAndMap(GET_MAX_LSN, rs->new Lsn(rs.getBytes(1)));
            if (null != lastLsn && lastLsn.isAvailable()) {
                snapshot.put(LSN_POSITION, lastLsn.toString());
                super.forceFlushEvent();
                return;
            }
            // Shouldn't happen if the agent is running, but it is better to guard against such situation
            throw new SqlServerException("No maximum LSN recorded in the database");
        }
        lastLsn = Lsn.valueOf(snapshot.get(LSN_POSITION));
    }

    private void readTables() {
        tables = queryAndMapList(GET_TABLE_LIST.replace(STATEMENTS_PLACEHOLDER, schema), rs-> {
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
        changeTables = queryAndMapList(GET_TABLES_CDC_ENABLED, rs-> {
            final Set<SqlServerChangeTable> changed = new HashSet<>();
            while (rs.next()) {
                // 只关注当前任务监听的表，避免拉取无关表的变更数据
                if (!this.tables.contains(rs.getString(2))) {
                    continue;
                }
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
                changed.add(changeTable);
            }
            return changed;
        });
    }

    private void enableTableCDC() {
        if (!CollectionUtils.isEmpty(tables)) {
            tables.forEach(table-> {
                boolean enabledTableCDC = queryAndMap(IS_TABLE_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, table), rs->rs.getInt(1) > 0);
                if (!enabledTableCDC) {
                    execute(String.format(ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, table), schema));
                    Lsn minLsn = queryAndMap(GET_MIN_LSN.replace(STATEMENTS_PLACEHOLDER, table), rs->new Lsn(rs.getBytes(1)));
                    logger.info("启用CDC表[{}]:{}", table, minLsn.isAvailable());
                }
            });
        }
    }

    private void enableDBCDC() throws InterruptedException {
        String realDatabaseName = queryAndMap(GET_DATABASE_NAME, rs->rs.getString(1));
        boolean enabledCDC = queryAndMap(IS_DB_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, realDatabaseName), rs->rs.getBoolean(1));
        if (!enabledCDC) {
            execute(ENABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, realDatabaseName));
            // make sure it works
            TimeUnit.SECONDS.sleep(3);

            enabledCDC = queryAndMap(IS_DB_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, realDatabaseName), rs->rs.getBoolean(1));
            Assert.isTrue(enabledCDC, "Please ensure that the SQL Server Agent is running");
        }
    }

    private void execute(String... sqlStatements) {
        instance.execute(databaseTemplate-> {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) {
                    logger.info("executing '{}'", sqlStatement);
                    databaseTemplate.execute(sqlStatement);
                }
            }
            return true;
        });
    }

    private long pull(Lsn stopLsn) {
        final Lsn startLsn = queryAndMap(GET_INCREMENT_LSN, statement->statement.setBytes(1, lastLsn.getBinary()), rs->Lsn.valueOf(rs.getBytes(1)));
        if (null == startLsn || !startLsn.isAvailable()) {
            throw new SqlServerException("获取起始LSN失败, lastLsn=" + lastLsn);
        }
        long rows = 0;
        for (SqlServerChangeTable changeTable : changeTables) {
            Lsn fromLsn = resolveFromLsn(changeTable, startLsn, stopLsn);
            if (null == fromLsn) {
                continue;
            }
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            Integer count = queryAndMapList(query, statement-> {
                statement.setFetchSize(FETCH_SIZE);
                statement.setBytes(1, fromLsn.getBinary());
                statement.setBytes(2, stopLsn.getBinary());
            }, rs->parseEvent(changeTable.getTableName(), rs, stopLsn));
            // Msg 313按空结果处理；其它异常会向上抛出，阻止位点推进
            if (null != count) {
                rows += count;
            }
        }
        return rows;
    }

    /**
     * 将拉取起点钳制到捕获实例的最小可用LSN，避免因CDC清理导致起点越界触发Msg 313。
     * <p>若起点已越过本批结束位点，则本表本批无需拉取。
     *
     * @return 有效起点；无需拉取时返回null
     */
    private Lsn resolveFromLsn(SqlServerChangeTable changeTable, Lsn startLsn, Lsn stopLsn) {
        Lsn fromLsn = startLsn;
        Lsn minLsn = queryAndMap(GET_MIN_LSN.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance()),
                rs->Lsn.valueOf(rs.getBytes(1)));
        if (null != minLsn && minLsn.isAvailable() && fromLsn.compareTo(minLsn) < 0) {
            logger.warn("CDC位点[{}]早于表[{}]最小可用LSN[{}]，可能因清理产生数据空洞，跳到最小LSN继续",
                    fromLsn, changeTable.getTableName(), minLsn);
            fromLsn = minLsn;
        }
        if (fromLsn.compareTo(stopLsn) > 0) {
            return null;
        }
        return fromLsn;
    }

    /**
     * 取出本批的结束位点，把大段积压拆成最多{@link #currentLsnRange}个事务的小批次。
     * <p>映射表查不到边界（如刚启用CDC、或积压已在目标位点内）时，直接推进到目标位点。
     */
    private Lsn nextBatchStopLsn(Lsn stopLsn) {
        Lsn batchStopLsn = queryAndMapList(GET_BATCH_STOP_LSN, statement-> {
            statement.setInt(1, currentLsnRange);
            statement.setBytes(2, lastLsn.getBinary());
            statement.setBytes(3, stopLsn.getBinary());
        }, rs->rs.next() ? Lsn.valueOf(rs.getBytes(1)) : null);
        if (null == batchStopLsn || !batchStopLsn.isAvailable()) {
            return stopLsn;
        }
        // 必须严格前进，否则回退到目标位点，避免批次不推进
        boolean valid = batchStopLsn.compareTo(lastLsn) > 0 && batchStopLsn.compareTo(stopLsn) < 0;
        return valid ? batchStopLsn : stopLsn;
    }

    /**
     * 根据上一批的行数和耗时动态调整批次范围：过重则缩小以加快位点刷新，过轻则放大以加快追平积压。
     */
    private void adjustLsnRange(long rows, long duration) {
        int newRange = currentLsnRange;
        if (duration > SLOW_PULL_MILLIS || rows > HIGH_PULL_ROWS) {
            newRange = Math.max(MIN_LSN_RANGE, currentLsnRange / 2);
        } else if (duration < FAST_PULL_MILLIS && rows < HIGH_PULL_ROWS / 10) {
            newRange = Math.min(MAX_LSN_RANGE, currentLsnRange * 2);
        }
        if (newRange != currentLsnRange) {
            logger.info("调整CDC批次范围[{} -> {}], 本批行数:{}, 耗时:{}ms", currentLsnRange, newRange, rows, duration);
            currentLsnRange = newRange;
        }
    }

    private void trySendEvent(RowChangedEvent event) {
        while (connected) {
            try {
                sendChangedEvent(event);
                break;
            } catch (QueueOverflowException ex) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException exe) {
                    logger.error(exe.getMessage(), exe);
                }
            }
        }
    }

    /**
     * 边读边发，避免变更量过大时一次性堆积到内存导致OOM。
     * 仅缓存一行做前瞻，用于给最后一行附加stopLsn位点。
     */
    private int parseEvent(String tableName, ResultSet rs, Lsn stopLsn) throws SQLException {
        final int columnCount = rs.getMetaData().getColumnCount();
        CDCEvent pending = null;
        int rows = 0;
        while (rs.next()) {
            // skip update before
            final int operation = rs.getInt(3);
            if (TableOperationEnum.isUpdateBefore(operation)) {
                continue;
            }
            List<Object> row = new ArrayList<>(columnCount - OFFSET_COLUMNS);
            for (int i = OFFSET_COLUMNS + 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            if (null != pending) {
                sendEvent(pending, null);
            }
            pending = new CDCEvent(tableName, operation, row);
            rows++;
        }
        if (null != pending) {
            sendEvent(pending, stopLsn);
        }
        return rows;
    }

    private void sendEvent(CDCEvent event, Lsn offset) {
        if (TableOperationEnum.isUpdateAfter(event.getCode())) {
            trySendEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_UPDATE, event.getRow(), null, offset));
            return;
        }

        if (TableOperationEnum.isInsert(event.getCode())) {
            trySendEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_INSERT, event.getRow(), null, offset));
            return;
        }

        if (TableOperationEnum.isDelete(event.getCode())) {
            trySendEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_DELETE, event.getRow(), null, offset));
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
        return query(sql, statementPreparer, (rs)-> {
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
        return instance.execute(databaseTemplate-> {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = databaseTemplate.getSimpleConnection().prepareStatement(preparedQuerySql);
                if (null != statementPreparer) {
                    statementPreparer.accept(ps);
                }
                rs = ps.executeQuery();
                return mapper.apply(rs);
            } catch (SQLException e) {
                // Msg 313: CDC TVF在LSN为空/越界时的误导性报错，按空结果处理；其它异常必须抛出，避免位点误推进丢数
                if (isCdcInvalidLsnError(e)) {
                    logger.warn("CDC查询LSN区间无效，按空结果处理. sql={}, error={}", preparedQuerySql, e.getMessage());
                    return null;
                }
                throw new SqlServerException(e);
            } finally {
                close(rs);
                close(ps);
            }
        });
    }

    /**
     * 判定是否为CDC变更函数因LSN区间无效抛出的Msg 313。
     * <p>不能仅按消息包含函数名判断，否则会把权限拒绝(Msg 229)等真实错误误当成空结果。
     */
    private boolean isCdcInvalidLsnError(SQLException e) {
        if (e.getErrorCode() == CDC_INVALID_LSN_ERROR) {
            return true;
        }
        // 部分驱动/本地化场景下errorCode可能为0，再用文案兜底
        String message = e.getMessage();
        return null != message
                && (message.contains("参数数目不足") || message.contains("insufficient number of arguments"))
                && (message.contains("fn_cdc_get_all_changes") || message.contains("fn_cdc_get_net_changes"));
    }

    public Lsn getMaxLsn() {
        return queryAndMap(GET_MAX_LSN, rs->new Lsn(rs.getBytes(1)));
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

                    // 分批推进到目标位点，避免单批变更过大，同时更频繁地刷新位点
                    while (connected && !isInterrupted() && lastLsn.compareTo(stopLsn) < 0) {
                        Lsn batchStopLsn = nextBatchStopLsn(stopLsn);
                        long begin = System.currentTimeMillis();
                        long rows = pull(batchStopLsn);

                        lastLsn = batchStopLsn;
                        snapshot.put(LSN_POSITION, lastLsn.toString());
                        adjustLsnRange(rows, System.currentTimeMillis() - begin);
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Throwable e) {
                    // 捕获Error(如OOM)，避免解析线程静默退出后任务假死
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
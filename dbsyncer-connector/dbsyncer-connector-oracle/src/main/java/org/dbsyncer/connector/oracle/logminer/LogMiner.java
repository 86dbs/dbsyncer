/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer;

import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.sdk.util.DatabaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-09 20:21
 */
public class LogMiner {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Lock lock = new ReentrantLock();
    private final String username;
    private final String password;
    private final String url;
    private final String schema;
    private final String driverClassName;
    private final int queryTimeout = 300;
    
    // 动态 FetchSize 配置
    private final int minFetchSize = 100;
    private final int maxFetchSize = 5000;
    private volatile int currentFetchSize = 1000;
    
    // 动态休眠时间配置（毫秒）
    private final int minSleepMillis = 100;
    private final int maxSleepMillis = 3000;
    private volatile int currentSleepMillis = 1000;
    
    // 查询范围控制
    private final long MAX_SCN_RANGE = 10000;
    
    // 性能统计
    private volatile long lastQueryRows = 0;
    private volatile long consecutiveEmptyQueries = 0;
    private volatile long totalQueriesCount = 0;
    private volatile long totalRowsProcessed = 0;
    private volatile long totalEmptyQueries = 0;
    private volatile long lastStatsTime = System.currentTimeMillis();
    
    private volatile boolean connected = false;
    private Connection connection;
    private List<BigInteger> currentRedoLogSequences;
    private final TransactionalBuffer transactionalBuffer = new TransactionalBuffer();
    // 已提交的位点
    private Long committedScn = 0L;
    // 初始位点
    private long startScn = 0;
    // 本轮查询处理的最大SCN，用于避免重复查询
    private long processedScn = 0;
    private EventListener listener;
    private Worker worker;

    public LogMiner(String username, String password, String url, String schema, String driverClassName) {
        this.username = username;
        this.password = password;
        this.url = url;
        this.schema = schema;
        this.driverClassName = driverClassName;
    }

    public void close() {
        connected = false;
        closeQuietly();
    }

    private void closeQuietly() {
        if (isValid()) {
            LogMinerHelper.endLogMiner(connection);
        }
        if (null != worker && !worker.isInterrupted()) {
            worker.interrupt();
            worker = null;
        }
        close(connection);
    }

    public void start() throws SQLException {
        boolean locked = false;
        try {
            locked = lock.tryLock(5, TimeUnit.SECONDS);
            if (locked && !connected) {
                connected = true;
                connect();
            } else {
                logger.error("LogMiner is already started");
            }
        } catch (InterruptedException e) {
            throw new OracleException(e);
        } finally {
            if (locked) {
                lock.unlock();
            }
        }
    }

    private Connection createConnection() throws SQLException {
        return DatabaseUtil.getConnection(driverClassName, url, username, password);
    }

    private Connection validateConnection() throws SQLException {
        Connection conn = null;
        try {
            conn = DatabaseUtil.getConnection(driverClassName, url, username, password);
            LogMinerHelper.setSessionParameter(conn);
            int version = conn.getMetaData().getDatabaseMajorVersion();
            // 19支持cdb模式
            if (version == 19) {
                LogMinerHelper.setSessionContainerIfCdbMode(conn);
            }
            // 低于10不支持
            else if (version < 10) {
                throw new IllegalArgumentException(String.format("Unsupported database version: %d(current) < 10", version));
            }
            // 检查账号权限
            LogMinerHelper.checkPermissions(conn, version);
        } catch (Exception e) {
            close(conn);
            throw e;
        }
        return conn;
    }

    private void connect() throws SQLException {
        this.connection = validateConnection();
        // 判断是否第一次读取
        if (startScn == 0) {
            startScn = LogMinerHelper.getCurrentScn(connection);
            restartLogMiner(startScn);
        } else {
            restartLogMiner(LogMinerHelper.getCurrentScn(connection));
        }
        logger.info("Start log miner, scn={}", startScn);
        worker = new Worker();
        worker.setName("log-miner-parser-" + url + "_" + worker.hashCode());
        worker.setDaemon(false);
        worker.start();
    }

    private void recover() {
        logger.error("Connection interrupted, attempting to reconnect");
        while (connected) {
            try {
                closeQuietly();
                connect();
                logger.info("Reconnect successfully");
                break;
            } catch (Exception e) {
                logger.error("Reconnect failed", e);
                sleepSeconds(5);
            }
        }
    }

    private void sleepSeconds(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            if (connected) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private void restartLogMiner(long endScn) throws SQLException {
        LogMinerHelper.startLogMiner(connection, startScn, endScn);
        currentRedoLogSequences = LogMinerHelper.getCurrentRedoLogSequences(connection);
    }

    private boolean redoLogSwitchOccurred() throws SQLException {
        final List<BigInteger> newSequences = LogMinerHelper.getCurrentRedoLogSequences(connection);
        if (!newSequences.equals(currentRedoLogSequences)) {
            currentRedoLogSequences = newSequences;
            return true;
        }
        return false;
    }

    private void logMinerViewProcessor(ResultSet rs) throws SQLException {
        long rowCount = 0;
        while (rs.next()) {
            rowCount++;
            BigInteger scn = rs.getBigDecimal("SCN").toBigInteger();
            String tableName = rs.getString("TABLE_NAME");
            String segOwner = rs.getString("SEG_OWNER");
            int operationCode = rs.getInt("OPERATION_CODE");
            Timestamp changeTime = rs.getTimestamp("TIMESTAMP");
            String txId = rs.getString("XID");
            if (scn.longValue() > processedScn) {
                processedScn = scn.longValue();
            }
            // Commit
            if (operationCode == LogMinerHelper.LOG_MINER_OC_COMMIT) {
                // 将TransactionalBuffer中当前事务的DML 转移到消费者处理
                transactionalBuffer.commit(txId, scn, committedScn);
                continue;
            }

            // Rollback
            if (operationCode == LogMinerHelper.LOG_MINER_OC_ROLLBACK) {
                // 清空TransactionalBuffer中当前事务
                if (transactionalBuffer.rollback(txId)) {
                    logger.info("txId: {} rollback", txId);
                }
                continue;
            }

            // MISSING_SCN
            if (operationCode == LogMinerHelper.LOG_MINER_OC_MISSING_SCN) {
                logger.warn("Found MISSING_SCN");
                continue;
            }

            // DDL
            String redoSql = getRedoSQL(rs);
            if (operationCode == LogMinerHelper.LOG_MINER_OC_DDL) {
                updateCommittedScn(scn.longValue());
                listener.onEvent(new RedoEvent(scn.longValue(), operationCode, redoSql, segOwner, tableName, changeTime, txId));
                continue;
            }

            // DML
            if (operationCode == LogMinerHelper.LOG_MINER_OC_INSERT
                    || operationCode == LogMinerHelper.LOG_MINER_OC_DELETE
                    || operationCode == LogMinerHelper.LOG_MINER_OC_UPDATE) {
                // 内部维护 TransactionalBuffer，将每条DML注册到Buffer中
                // 根据事务提交或者回滚情况决定如何处理
                if (redoSql != null) {
                    final RedoEvent event = new RedoEvent(scn.longValue(), operationCode, redoSql, segOwner, tableName, changeTime, txId);
                    // Transactional Commit Callback
                    TransactionalBuffer.CommitCallback commitCallback = (smallestScn, commitScn, counter) -> {
                        if (smallestScn == null || scn.compareTo(smallestScn) < 0) {
                            // 当前SCN 事务已经提交 并且 小于事务缓冲区中所有的开始SCN，所以可以更新offsetScn
                            startScn = scn.longValue();
                        }
                        if (counter == 0) {
                            updateCommittedScn(commitScn.longValue());
                        }
                        event.setScn(startScn < committedScn ? committedScn : startScn);
                        listener.onEvent(event);
                    };
                    // 生成操作唯一标识：scn + 表名 + 操作类型 + SQL内容hash
                    // 用于防止重复查询导致的重复处理（同一事务的不同DML可能有相同SCN，所以不能只用SCN判断）
                    String operationId = scn + "_" + tableName + "_" + operationCode + "_" + redoSql.hashCode();
                    transactionalBuffer.registerCommitCallback(txId, scn, operationId, commitCallback);
                }
            }
        }
        // 保存本轮查询行数，用于动态调整
        lastQueryRows = rowCount;
    }

    private void updateCommittedScn(long newScn) {
        committedScn = newScn > committedScn ? newScn : committedScn;
    }
    
    /**
     * 动态调整 FetchSize
     * 根据查询结果动态调整，提升性能和内存利用率
     */
    private void adjustFetchSize() {
        if (lastQueryRows == 0) {
            consecutiveEmptyQueries++;
            // 连续空查询，降低 fetchSize 节省内存
            if (consecutiveEmptyQueries > 10 && currentFetchSize > minFetchSize) {
                currentFetchSize = Math.max(minFetchSize, currentFetchSize / 2);
                logger.debug("Reduce fetchSize to {} after {} consecutive empty queries",
                    currentFetchSize, consecutiveEmptyQueries);
            }
        } else {
            consecutiveEmptyQueries = 0;
            // 有数据，根据查询行数动态调整
            if (lastQueryRows >= currentFetchSize * 0.9) {
                // 接近满载，增加 fetchSize
                int newSize = Math.min(maxFetchSize, currentFetchSize * 2);
                if (newSize != currentFetchSize) {
                    currentFetchSize = newSize;
                    logger.debug("Increase fetchSize to {} (utilization: {}%)", 
                        currentFetchSize, (lastQueryRows * 100 / currentFetchSize));
                }
            } else if (lastQueryRows < currentFetchSize * 0.3 && currentFetchSize > minFetchSize) {
                // 利用率低，降低 fetchSize
                int newSize = Math.max(minFetchSize, (int)(lastQueryRows * 1.5));
                if (newSize != currentFetchSize) {
                    currentFetchSize = newSize;
                    logger.debug("Adjust fetchSize to {} based on actual rows {}", 
                        currentFetchSize, lastQueryRows);
                }
            }
        }
    }
    
    /**
     * 动态调整休眠时间
     * 根据数据量灵活调整轮询间隔，平衡延迟和CPU占用
     */
    private void adjustSleepTime() {
        if (lastQueryRows == 0) {
            // 无数据，逐步增加休眠时间
            int newSleep = Math.min(maxSleepMillis, currentSleepMillis + 200);
            if (newSleep != currentSleepMillis) {
                currentSleepMillis = newSleep;
                logger.debug("Increase sleep time to {}ms (no data)", currentSleepMillis);
            }
        } else if (lastQueryRows >= currentFetchSize * 0.8) {
            // 接近满载，缩短休眠时间，快速处理积压
            if (currentSleepMillis > minSleepMillis) {
                currentSleepMillis = minSleepMillis;
                logger.debug("Reduce sleep time to {}ms (high load)", currentSleepMillis);
            }
        } else {
            // 有数据但不多，适度休眠
            int newSleep = Math.max(minSleepMillis, Math.min(maxSleepMillis, 500));
            if (Math.abs(newSleep - currentSleepMillis) > 100) {
                currentSleepMillis = newSleep;
                logger.debug("Adjust sleep time to {}ms (moderate load)", currentSleepMillis);
            }
        }
    }
    
    /**
     * 收集性能统计信息
     * 每分钟输出一次统计报告
     */
    private void collectStatistics() {
        totalQueriesCount++;
        totalRowsProcessed += lastQueryRows;
        if (lastQueryRows == 0) {
            totalEmptyQueries++;
        }
        
        long now = System.currentTimeMillis();
        if (now - lastStatsTime > 60000) {  // 每分钟输出一次
            long avgRows = totalQueriesCount > 0 ? totalRowsProcessed / totalQueriesCount : 0;
            long emptyRate = totalQueriesCount > 0 ? totalEmptyQueries * 100 / totalQueriesCount : 0;
            
            logger.info("=== LogMiner Performance Stats (1min) ===");
            logger.info("Queries: {}, Rows: {}, Empty: {}", 
                totalQueriesCount, totalRowsProcessed, totalEmptyQueries);
            logger.info("Avg rows/query: {}, Empty rate: {}%", avgRows, emptyRate);
            logger.info("Current fetchSize: {}, sleep: {}ms", currentFetchSize, currentSleepMillis);
            logger.info("Current SCN: {}, Committed SCN: {}, Buffer size: {}", 
                startScn, committedScn, transactionalBuffer.isEmpty() ? 0 : "non-zero");
            logger.info("=========================================");
            
            // 重置统计（滚动窗口）
            totalQueriesCount = 0;
            totalRowsProcessed = 0;
            totalEmptyQueries = 0;
            lastStatsTime = now;
        }
    }

    private String getRedoSQL(ResultSet rs) throws SQLException {
        String redoSql = rs.getString("SQL_REDO");
        if (redoSql == null) {
            return null;
        }
        StringBuilder redoBuilder = new StringBuilder(redoSql);

        // https://docs.oracle.com/cd/B19306_01/server.102/b14237/dynviews_1154.htm#REFRN30132
        // Continuation SQL flag. Possible values are:
        // 0 = indicates SQL_REDO and SQL_UNDO is contained within the same row
        // 1 = indicates that either SQL_REDO or SQL_UNDO is greater than 4000 bytes in size and is continued in the next row returned by the view
        int csf = rs.getInt("CSF");

        while (csf == 1) {
            rs.next();
            redoBuilder.append(rs.getString("SQL_REDO"));
            csf = rs.getInt("CSF");
        }

        return redoBuilder.toString();
    }

    public void registerEventListener(EventListener listener) {
        this.listener = listener;
    }

    public long getStartScn() {
        return startScn;
    }

    public void setStartScn(long startScn) {
        this.startScn = startScn;
    }

    public interface EventListener {

        void onEvent(RedoEvent redoEvent);
    }

    public boolean isConnected() {
        return connected;
    }

    // 判断连接是否正常
    private boolean isValid() {
        try {
            return connection != null && connection.isValid(queryTimeout);
        } catch (SQLException e) {
            return false;
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
    private long getNewStartScn(Long smallestUncommittedScn, long endScn) {
        long newStartScn;
        if (smallestUncommittedScn != null && committedScn > smallestUncommittedScn) {
            // 【关键保护】有未提交事务，且committedScn已经超过该事务的起始SCN
            // 如果推进到committedScn，会跳过未提交事务的后续操作
            // 只能推进到processedScn+1（已读取的最大SCN），不能跳跃
            if (processedScn > startScn) {
                newStartScn = processedScn + 1;
            } else {
                // 本轮没有新数据，保持不变，等待未提交事务的后续操作
                newStartScn = startScn;
            }
        } else if (committedScn > startScn) {
            // 有事务提交，且没有更早的未提交事务阻塞
            // 安全推进到committedScn
            newStartScn = committedScn;
        } else if (processedScn > startScn) {
            // 本轮读取了数据，但没有提交
            // 推进到processedScn+1，避免重复查询
            newStartScn = processedScn + 1;
        } else {
            // 没有读取到任何数据（processedScn == startScn）
            // 【关键】不能直接跳到endScn！Oracle LogMiner的归档数据可能有延迟出现在V$LOGMNR_CONTENTS
            // 平衡策略：
            // - 如果距离很近（<10），可能是归档延迟，保守推进
            // - 如果距离较远（>=10），可能是过滤条件导致的空窗口，适度推进
            long distance = endScn - startScn;
            if (distance < 10) {
                // 距离太近，可能有归档延迟，只推进一小步
                newStartScn = startScn + 1;
            } else {
                // 距离较远，适度推进到中间位置，平衡性能与安全性
                newStartScn = startScn + Math.min(distance / 2, 100);
            }
        }
        return newStartScn;
    }

    private PreparedStatement createStatement() throws SQLException {
        String minerViewQuery = LogMinerHelper.logMinerViewQuery(schema, username);
        PreparedStatement statement = connection.prepareStatement(
                minerViewQuery,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        statement.setQueryTimeout(queryTimeout);
        return statement;
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            PreparedStatement cachedStatement = null;
            ResultSet rs = null;
            try {
                cachedStatement = createStatement();
                while (!isInterrupted() && connected) {
                    // 1. 检查连接有效性
                    if (!isValid()) {
                        close(cachedStatement);
                        connection = createConnection();
                        cachedStatement = createStatement();
                    }
                    
                    // 关闭上一轮的 ResultSet（但复用 PreparedStatement）
                    close(rs);
                    
                    // 2. 确定查询范围（控制单次查询的 SCN 跨度）
                    long currentScn = LogMinerHelper.getCurrentScn(connection);
                    long endScn = Math.min(currentScn, startScn + MAX_SCN_RANGE);
                    
                    // 检测积压情况
                    long backlog = currentScn - startScn;
                    if (backlog > MAX_SCN_RANGE * 2) {
                        logger.warn("Large SCN backlog detected: {}, current processing may be slow", backlog);
                    }

                    // 3. 检查 Redo Log 切换
                    if (redoLogSwitchOccurred()) {
                        logger.info("Switch to new redo log");
                        restartLogMiner(endScn);
                    }

                    // 4. 查询 LogMiner 视图（复用 PreparedStatement）
                    processedScn = startScn;
                    
                    // 5. 动态设置 fetchSize
                    cachedStatement.setFetchSize(currentFetchSize);
                    cachedStatement.setString(1, String.valueOf(startScn));
                    cachedStatement.setString(2, String.valueOf(endScn));
                    
                    try {
                        rs = cachedStatement.executeQuery();
                        logMinerViewProcessor(rs);
                    } catch (SQLException e) {
                        if (e.getMessage().contains("ORA-00310")) {
                            logger.info("ORA-00310 restart log miner");
                            LogMinerHelper.endLogMiner(connection);
                            restartLogMiner(endScn);
                            continue;
                        }
                        throw e;
                    }
                    
                    // 6. 推进 SCN
                    Long smallestUncommittedScn = transactionalBuffer.getSmallestScn();
                    long newStartScn = getNewStartScn(smallestUncommittedScn, endScn);
                    if (newStartScn != startScn) {
                        startScn = newStartScn;
                    }
                    
                    // 7. 性能统计和动态调整
                    collectStatistics();
                    adjustFetchSize();
                    adjustSleepTime();
                    
                    // 8. 动态休眠
                    try {
                        TimeUnit.MILLISECONDS.sleep(currentSleepMillis);
                    } catch (InterruptedException e) {
                        if (connected) {
                            logger.error("Sleep interrupted: {}", e.getMessage());
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                if (connected) {
                    logger.error(e.getMessage(), e);
                    recover();
                }
            } finally {
                close(rs);
                close(cachedStatement);
            }
        }
    }
}
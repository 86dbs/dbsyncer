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
import java.sql.Statement;
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
    private final int fetchSize = 1000;
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
        while (rs.next()) {
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
            logger.info("txId:{}, table:{}, operationCode:{}, changeTime:{}, scn:{}", txId, tableName, operationCode, changeTime, scn);
            if (operationCode == LogMinerHelper.LOG_MINER_OC_COMMIT) {
                // 将TransactionalBuffer中当前事务的DML 转移到消费者处理
                if (transactionalBuffer.commit(txId, scn, committedScn)) {
                    logger.info("txId: {} commit", txId);
                }
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
            logger.info("operationCode:{}, txId:{}, redoSql:{}", operationCode, txId, redoSql);
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
    }

    private void updateCommittedScn(long newScn) {
        committedScn = newScn > committedScn ? newScn : committedScn;
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

    /** 关闭数据库连接资源 */
    private void closeResources(ResultSet rs, Statement stmt) {
        close(rs);
        close(stmt);
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

    final class Worker extends Thread {

        @Override
        public void run() {
            String minerViewQuery = LogMinerHelper.logMinerViewQuery(schema, username);
            PreparedStatement statement = null;
            ResultSet rs = null;
            try {
                while (!isInterrupted() && connected) {
                    if (!isValid()) {
                        connection = createConnection();
                    }
                    closeResources(rs, statement);
                    // 1.确定 endScn
                    long endScn = LogMinerHelper.getCurrentScn(connection);

                    // 2.是否发生redoLog切换
                    if (redoLogSwitchOccurred()) {
                        logger.info("Switch to new redo log");
                        restartLogMiner(endScn);
                    }

                    // 3.查询 logMiner view, 处理结果集
                    // 重置本轮处理的SCN追踪
                    processedScn = startScn;
                    statement = connection.prepareStatement(minerViewQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    statement.setFetchSize(fetchSize);
                    statement.setFetchDirection(ResultSet.FETCH_FORWARD);
                    statement.setQueryTimeout(queryTimeout);
                    statement.setString(1, String.valueOf(startScn));
                    statement.setString(2, String.valueOf(endScn));
                    try {
                        rs = statement.executeQuery();
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
                    // 4.确定新的SCN
                    Long smallestUncommittedScn = transactionalBuffer.getSmallestScn();
                    long newStartScn = getNewStartScn(smallestUncommittedScn, endScn);
                    if (newStartScn != startScn) {
                        startScn = newStartScn;
                    }
                    sleepSeconds(1);
                }
            } catch (Exception e) {
                if (connected) {
                    logger.error(e.getMessage(), e);
                    recover();
                }
            } finally {
                closeResources(rs, statement);
            }
        }
    }
}
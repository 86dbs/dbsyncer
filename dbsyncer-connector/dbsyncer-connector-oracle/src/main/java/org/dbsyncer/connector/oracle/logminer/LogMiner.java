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
                //LogMinerHelper.setSessionContainerIfCdbMode(conn);
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
            // Commit
            if (operationCode == LogMinerHelper.LOG_MINER_OC_COMMIT) {
                // 将TransactionalBuffer中当前事务的DML 转移到消费者处理
                if (transactionalBuffer.commit(txId, scn, committedScn)) {
                    logger.debug("txId: {} commit", txId);
                }
                continue;
            }

            // Rollback
            if (operationCode == LogMinerHelper.LOG_MINER_OC_ROLLBACK) {
                // 清空TransactionalBuffer中当前事务
                if (transactionalBuffer.rollback(txId)) {
                    logger.debug("txId: {} rollback", txId);
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
                    transactionalBuffer.registerCommitCallback(txId, scn, commitCallback);
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
                    startScn = endScn;
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
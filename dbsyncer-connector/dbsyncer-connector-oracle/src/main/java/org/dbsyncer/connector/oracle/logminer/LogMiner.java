/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer;

import org.apache.commons.lang3.time.StopWatch;
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
import java.util.stream.Collectors;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-09 20:21
 */
public class LogMiner {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Lock lock = new ReentrantLock();
    private String username;
    private String password;
    private String url;
    private String schema;
    private String driverClassName;
    private String miningStrategy = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG";
    private volatile boolean connected = false;
    private final StopWatch stopWatch = StopWatch.create();
    private Connection connection;
    private List<BigInteger> currentRedoLogSequences;
    private TransactionalBuffer transactionalBuffer = new TransactionalBuffer();
    // 已提交的位点
    private Long committedScn = 0L;
    // 初始位点
    private long startScn = 0;
    private EventListener listener;

    public LogMiner(String username, String password, String url, String schema, String driverClassName) {
        this.username = username;
        this.password = password;
        this.url = url;
        this.schema = schema;
        this.driverClassName = driverClassName;
    }

    public void close() throws SQLException {
        lock.lock();
        if (!connected) {
            logger.error("LogMiner is already stop");
            lock.unlock();
            return;
        }
        this.connection.close();
        connected = false;
        lock.unlock();
    }

    public void start() throws SQLException {
        lock.lock();
        if (connected) {
            logger.error("LogMiner is already started");
            lock.unlock();
            return;
        }
        this.connection = DatabaseUtil.getConnection(driverClassName, url, username, password);
        connected = true;
        lock.unlock();
        //get current scn 判断是否第一次没有存储
        if (startScn == 0) {
            startScn = getCurrentScn(connection);
        }

        logger.info("scn start '{}'", startScn);
        logger.info("start LogMiner...");
        LogMinerHelper.setSessionParameter(connection);

        // 1.记录当前redoLog，用于下文判断redoLog 是否切换
        currentRedoLogSequences = LogMinerHelper.getCurrentRedoLogSequences(connection);

        // 2.构建数据字典 && add redo / archived log
        initializeLogMiner();

        String minerViewQuery = LogMinerHelper.logMinerViewQuery(schema, username);
        logger.debug(minerViewQuery);

        try (PreparedStatement minerViewStatement = connection.prepareStatement(minerViewQuery, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
            // while
            while (connected) {
                // 3.确定 endScn
                BigInteger endScn = determineEndScn();

                // 4.是否发生redoLog切换
                if (redoLogSwitchOccurred()) {
                    // 如果切换则重启logMiner会话
                    logger.debug("restart LogMiner Session");
                    restartLogMiner();
                    currentRedoLogSequences = LogMinerHelper.getCurrentRedoLogSequences(connection);
                }

                // 5.start logMiner
                LogMinerHelper.startLogMiner(connection, BigInteger.valueOf(startScn), endScn, miningStrategy);

                // 6.查询 logMiner view, 处理结果集
                minerViewStatement.setFetchSize(2000);
                minerViewStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
                minerViewStatement.setString(1, String.valueOf(startScn));
                minerViewStatement.setString(2, endScn.toString());

                stopWatch.start();

                try (ResultSet rs = minerViewStatement.executeQuery()) {
                    logger.trace("Query V$LOGMNR_CONTENTS spend time {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
                    stopWatch.reset();
                    logMinerViewProcessor(rs);
                }

                // 7.确定新的SCN
                startScn = Long.parseLong(endScn.toString());

                try {
                    // 避免频繁的执行导致 PGA 内存超出 PGA_AGGREGATE_LIMIT
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public long getCurrentScn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select CURRENT_SCN from V$DATABASE");

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            return rs.getLong(1);
        }
    }

    private void restartLogMiner() throws SQLException {
        LogMinerHelper.endLogMiner(connection);
        initializeLogMiner();
    }

    private boolean redoLogSwitchOccurred() throws SQLException {
        final List<BigInteger> newSequences = LogMinerHelper.getCurrentRedoLogSequences(connection);
        if (!newSequences.equals(currentRedoLogSequences)) {
            currentRedoLogSequences = newSequences;
            return true;
        }
        return false;
    }

    private BigInteger determineEndScn() throws SQLException {
        return BigInteger.valueOf(getCurrentScn(connection));
    }

    private void initializeLogMiner() throws SQLException {
        // 默认使用在线数据字典，所以此处不做数据字典相关操作
        LogMinerHelper.buildDataDictionary(connection, miningStrategy);

        setRedoLog();
    }

    private void setRedoLog() throws SQLException {
        LogMinerHelper.removeLogFilesFromMining(connection);
        List<LogFile> onlineLogFiles = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, BigInteger.valueOf(startScn));
        List<LogFile> archivedLogFiles = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, BigInteger.valueOf(startScn));
        List<String> logFilesNames = archivedLogFiles.stream().map(LogFile::getFileName).collect(Collectors.toList());
        for (LogFile onlineLogFile : onlineLogFiles) {
            boolean found = false;
            for (LogFile archivedLogFile : archivedLogFiles) {
                if (onlineLogFile.isSameRange(archivedLogFile)) {
                    // 如果redo 已经被归档，那么就不需要加载这个redo了
                    found = true;
                    break;
                }
            }
            if (!found)
                logFilesNames.add(onlineLogFile.getFileName());
        }

        // 加载所需要的redo / archived
        for (String fileName : logFilesNames) {
            LogMinerHelper.addLogFile(connection, fileName);
        }
    }

    private void logMinerViewProcessor(ResultSet rs) throws SQLException {
        while (rs.next()) {
            BigInteger scn = rs.getBigDecimal("SCN").toBigInteger();
            String tableName = rs.getString("TABLE_NAME");
            String segOwner = rs.getString("SEG_OWNER");
            int operationCode = rs.getInt("OPERATION_CODE");
            Timestamp changeTime = rs.getTimestamp("TIMESTAMP");
            String txId = rs.getString("XID");
            String operation = rs.getString("OPERATION");
            String username = rs.getString("USERNAME");

            logger.trace("Capture record, SCN:{}, TABLE_NAME:{}, SEG_OWNER:{}, OPERATION_CODE:{}, TIMESTAMP:{}, XID:{}, OPERATION:{}, USERNAME:{}",
                    scn, tableName, segOwner, operationCode, changeTime, txId, operation, username);

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

                        event.setScn(committedScn);
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

    public void registerEventListener(EventListener listener){
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

}
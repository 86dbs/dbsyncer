/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer;

import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-09 20:23
 */
public class LogMinerHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);
    public static final int LOG_MINER_OC_INSERT = 1;
    public static final int LOG_MINER_OC_DELETE = 2;
    public static final int LOG_MINER_OC_UPDATE = 3;
    public static final int LOG_MINER_OC_DDL = 5;
    public static final int LOG_MINER_OC_COMMIT = 7;
    public static final int LOG_MINER_OC_MISSING_SCN = 34;
    public static final int LOG_MINER_OC_ROLLBACK = 36;

    public static void removeLogFilesFromMining(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS");
             ResultSet result = ps.executeQuery()) {
            Set<String> files = new LinkedHashSet<>();
            while (result.next()) {
                files.add(result.getString(1));
            }
            for (String fileName : files) {
                String sql = String.format("BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '%s');END;", fileName);
                executeCallableStatement(conn, sql);
                LOGGER.debug("File {} was removed from mining", fileName);
            }
        }
    }

    public static void executeCallableStatement(Connection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.prepareCall(statement)) {
            s.execute();
        }
    }

    public static List<LogFile> getOnlineLogFilesForOffsetScn(Connection connection, BigInteger offsetScn) throws SQLException {
        List<LogFile> redoLogFiles = new ArrayList<>();
        String onlineLogQuery = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.STATUS, L.FIRST_CHANGE# AS FIRST_CHANGE, L.NEXT_CHANGE# AS NEXT_CHANGE FROM V$LOG L, V$LOGFILE F WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 GROUP BY F.GROUP#, L.NEXT_CHANGE#, L.FIRST_CHANGE#, L.STATUS";
        try (PreparedStatement s = connection.prepareStatement(onlineLogQuery)) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    String fileName = rs.getString(1);// FILE_NAME
                    String status = rs.getString(2); // STATUS
                    BigInteger firstChangeNumber = new BigInteger(rs.getString(3));//FIRST_CHANGE
                    BigInteger nextChangeNumber = new BigInteger(rs.getString(4));//NEXT_CHANGE
                    LogFile logFile = new LogFile(fileName, firstChangeNumber, nextChangeNumber, "CURRENT".equalsIgnoreCase(status));
                    // 添加Current Redo || scn 范围符合的
                    if (logFile.isCurrent() || logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        redoLogFiles.add(logFile);
                    }
                }
            }
        }
        return redoLogFiles;
    }

    public static List<LogFile> get10GOnlineLogFilesForOffsetScn(Connection connection, BigInteger offsetScn) throws SQLException {
        List<LogFile> redoLogFiles = new ArrayList<>();
        String onlineLogQuery = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.STATUS, L.FIRST_CHANGE# AS FIRST_CHANGE FROM V$LOG L, V$LOGFILE F WHERE F.GROUP# = L.GROUP# GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.STATUS";
        try (PreparedStatement s = connection.prepareStatement(onlineLogQuery)) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    String fileName = rs.getString(1);// FILE_NAME
                    String status = rs.getString(2); // STATUS
                    BigInteger firstChangeNumber = new BigInteger(rs.getString(3));//FIRST_CHANGE
                    LogFile logFile = new LogFile(fileName, firstChangeNumber, null, "CURRENT".equalsIgnoreCase(status));
                    // 添加Current Redo || scn 范围符合的
                    if (logFile.isCurrent() || logFile.getFirstScn().compareTo(offsetScn) >= 0) {
                        redoLogFiles.add(logFile);
                    }
                }
            }
        }
        return redoLogFiles;
    }

    public static List<LogFile> getArchivedLogFilesForOffsetScn(Connection connection, BigInteger offsetScn) throws SQLException {
        String archiveLogsQuery = String.format("SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE FROM V$ARCHIVED_LOG " +
                "WHERE NAME IS NOT NULL AND ARCHIVED = 'YES' " +
                "AND STATUS = 'A' AND NEXT_CHANGE# > %s ORDER BY 2", offsetScn);

        final List<LogFile> archiveLogFiles = new ArrayList<>();
        try (PreparedStatement s = connection.prepareStatement(archiveLogsQuery)) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    String fileName = rs.getString(1);
                    BigInteger firstChangeNumber = new BigInteger(rs.getString(3));
                    BigInteger nextChangeNumber = new BigInteger(rs.getString(2));
                    archiveLogFiles.add(new LogFile(fileName, firstChangeNumber, nextChangeNumber, false));
                }
            }
        }
        return archiveLogFiles;
    }

    public static void addLogFile(Connection connection, String fileName) throws SQLException {
        String addLogFile = "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '%s', OPTIONS => DBMS_LOGMNR.ADDFILE);END;";
        executeCallableStatement(connection, String.format(addLogFile, fileName));
    }

    public static List<BigInteger> getCurrentRedoLogSequences(Connection connection) throws SQLException {
        String currentRedoSequence = "SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'";
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(currentRedoSequence)) {
            List<BigInteger> sequences = new ArrayList<>();
            if (rs.next()) {
                sequences.add(new BigInteger(rs.getString(1)));
            }
            // 如果是RAC则会返回多个SEQUENCE
            return sequences;
        }
    }

    public static void buildDataDictionary(Connection connection, String miningStrategy) throws SQLException {
        if (StringUtil.isBlank(miningStrategy)) {
            // default
            String sql = "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
            executeCallableStatement(connection, sql);
        }
    }

    public static void startLogMiner(Connection connection, BigInteger startScn, BigInteger endScn, String miningStrategy) throws SQLException {
        LOGGER.debug("startLogMiner... startScn {}, endScn {}", startScn, endScn);
        // default
        if (StringUtil.isBlank(miningStrategy)) {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING ";
        }

        String startLogMiner = "BEGIN sys.dbms_logmnr.start_logmnr(" +
                "startScn => '" + startScn + "', " +
                "endScn => '" + endScn + "', " +
                "OPTIONS => " + miningStrategy +
                " + DBMS_LOGMNR.NO_ROWID_IN_STMT);" +
                "END;";

        executeCallableStatement(connection, startLogMiner);
    }

    public static void endLogMiner(Connection connection) {
        try {
            executeCallableStatement(connection, "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;");
        } catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.info("LogMiner session was already closed");
            } else {
                LOGGER.error("Cannot close LogMiner session gracefully: {}", e);
            }
        }
    }

    public static String logMinerViewQuery(String schema, String logMinerUser) {
        StringBuilder query = new StringBuilder();
//        query.append("SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME ");
        query.append("SELECT * ");
        query.append("FROM V$LOGMNR_CONTENTS ");
        query.append("WHERE ");
        // 这里由原来的 SCN > ? AND SCN <= ? 改为如下
        // 原因：
        // 在测试的时候发现一个情况会丢失部分数据
        // 结论：
        // START_SCN = X , END_SCN = Y, 此时查询条件 SCN >= X AND SCN <= Y
        // 查询 V$LOGMNR_CONTENTS, 此时如果SQL的SCN恰好等于Y, 那么这次可能不会查出SCN=Y 的SQL(并不是百分之百)
        // 但是当指定 SCN >= Y 时, 貌似一定能查到
        // 这个问题很奇怪，有待研究
        query.append("SCN >= ? AND SCN < ? ");
        query.append("AND (");
        // MISSING_SCN/DDL only when not performed by excluded users
        query.append("(OPERATION_CODE IN (5,34) AND USERNAME NOT IN (").append(getExcludedUsers(logMinerUser)).append(")) ");
        // COMMIT/ROLLBACK
        query.append("OR (OPERATION_CODE IN (7,36)) ");
        // INSERT/UPDATE/DELETE
        query.append("OR ");
        query.append("(OPERATION_CODE IN (1,2,3) ");
        query.append(" AND SEG_OWNER NOT IN ('APPQOSSYS','AUDSYS','CTXSYS','DVSYS','DBSFWUSER','DBSNMP','GSMADMIN_INTERNAL','LBACSYS','MDSYS','OJVMSYS','OLAPSYS','ORDDATA','ORDSYS','OUTLN','SYS','SYSTEM','WMSYS','XDB') ");

        if (StringUtil.isNotBlank(schema)) {
            query.append(String.format(" AND (REGEXP_LIKE(SEG_OWNER,'^%s$','i')) ", schema));
//            query.append(" AND ");
//            query.append("USERNAME = '");
//            query.append(schema);
//            query.append("' ");
        }

        query.append(" ))");

        return query.toString();
    }

    private static String getExcludedUsers(String logMinerUser) {
        return "'SYS','SYSTEM','" + logMinerUser.toUpperCase() + "'";
    }

    public static void setSessionParameter(Connection connection) throws SQLException {
        String sql = "ALTER SESSION SET "
                + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
                + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
                + "  NLS_NUMERIC_CHARACTERS = '.,'";

        executeCallableStatement(connection, sql);
        executeCallableStatement(connection, "ALTER SESSION SET TIME_ZONE = '00:00'");
    }

}
/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.OracleException;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-09 20:23
 */
public class LogMinerHelper {
    private static final Logger logger = LoggerFactory.getLogger(LogMinerHelper.class);
    public static final int LOG_MINER_OC_INSERT = 1;
    public static final int LOG_MINER_OC_DELETE = 2;
    public static final int LOG_MINER_OC_UPDATE = 3;
    public static final int LOG_MINER_OC_DDL = 5;
    public static final int LOG_MINER_OC_COMMIT = 7;
    public static final int LOG_MINER_OC_MISSING_SCN = 34;
    public static final int LOG_MINER_OC_ROLLBACK = 36;
    private static final String LOG_MINER_SQL_QUERY_ROLES = "SELECT * FROM USER_ROLE_PRIVS";
    private static final String LOG_MINER_KEY_GRANTED_ROLE = "GRANTED_ROLE";
    private static final String LOG_MINER_SQL_QUERY_PRIVILEGES = "SELECT * FROM SESSION_PRIVS";
    private static final String LOG_MINER_KEY_PRIVILEGE = "PRIVILEGE";
    private static final List<String> LOG_MINER_PRIVILEGES_NEEDED = Arrays.asList("SELECT_CATALOG_ROLE", "CREATE SESSION", "SELECT ANY TRANSACTION", "SELECT ANY DICTIONARY", "LOGMINING");
    private static final List<String> LOG_MINER_ORACLE_11_PRIVILEGES_NEEDED = Arrays.asList("SELECT_CATALOG_ROLE", "CREATE SESSION", "SELECT ANY TRANSACTION", "SELECT ANY DICTIONARY");
    private static final String LOG_MINER_DBA_ROLE = "DBA";
    private static final String LOG_MINER_SQL_GET_CURRENT_SCN = "select CURRENT_SCN from V$DATABASE";
    private static final String LOG_MINER_SQL_IS_CDB = "select cdb from v$database";
    private static final String LOG_MINER_SQL_ALTER_SESSION_CONTAINER = "alter session set container=CDB$ROOT";
    private static final String LOG_MINER_SQL_ALTER_NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
            + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
            + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
            + "  NLS_NUMERIC_CHARACTERS = '.,'"
            + "  TIME_ZONE = '00:00'";
    private static final String LOG_MINER_SQL_CURRENT_REDO_SEQUENCE = "SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'";
    private static final String LOG_MINER_SQL_END_LOG_MINER = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";
    private static final String LOG_MINER_SQL_START_LOG_MINER = "DECLARE\n" +
            "start_scn NUMBER := ?; end_scn NUMBER := ?; first_file BOOLEAN := true; \n" +
            "BEGIN \n" +
            "FOR log_file IN\n" +
            " (\n" +
            "  SELECT MIN(name) name, first_change# FROM \n" +
            "  (\n" +
            "   SELECT member AS name, first_change# FROM v$log l INNER JOIN v$logfile f ON l.group# = f.group# WHERE (l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE') AND first_change# < end_scn\n" +
            "   UNION\n" +
            "   SELECT name, first_change# FROM v$archived_log WHERE name IS NOT NULL AND STANDBY_DEST='NO' AND first_change# < end_scn AND next_change# > start_scn \n" +
            "  ) group by first_change# ORDER BY first_change# \n" +
            " ) LOOP \n" +
            " IF first_file THEN\n" +
            "  SYS.DBMS_LOGMNR.add_logfile(log_file.name, SYS.DBMS_LOGMNR.NEW);\n" +
            "  first_file := false;\n" +
            " ELSE\n" +
            "  SYS.DBMS_LOGMNR.add_logfile(log_file.name, SYS.DBMS_LOGMNR.ADDFILE);\n" +
            " END IF;\n" +
            "END LOOP;\n" +
            "\n" +
            "SYS.DBMS_LOGMNR.start_logmnr( options => SYS.DBMS_LOGMNR.SKIP_CORRUPTION + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);\n" +
            "END;";

    public static void executeCallableStatement(Connection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.prepareCall(statement)) {
            s.execute();
        }
    }

    public static List<BigInteger> getCurrentRedoLogSequences(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(LOG_MINER_SQL_CURRENT_REDO_SEQUENCE)) {
                List<BigInteger> sequences = new ArrayList<>();
                if (rs.next()) {
                    sequences.add(new BigInteger(rs.getString(1)));
                }
                // 如果是RAC则会返回多个SEQUENCE
                return sequences;
            }
        }
    }

    public static void endLogMiner(Connection connection) {
        if (connection != null) {
            try {
                executeCallableStatement(connection, LOG_MINER_SQL_END_LOG_MINER);
            } catch (Exception e) {
                if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                    logger.info("LogMiner session was already closed", e);
                } else {
                    logger.warn("Cannot close log miner session gracefully", e);
                }
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
        executeCallableStatement(connection, LOG_MINER_SQL_ALTER_NLS_SESSION_PARAMETERS);
    }

    public static void startLogMiner(Connection connection, long startScn, long endScn) throws SQLException {
        try (PreparedStatement logMinerStartStmt = connection.prepareCall(LOG_MINER_SQL_START_LOG_MINER)) {
            logMinerStartStmt.setString(1, String.valueOf(startScn));
            logMinerStartStmt.setString(2, String.valueOf(endScn));
            logMinerStartStmt.execute();
        }
    }

    public static long getCurrentScn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(LOG_MINER_SQL_GET_CURRENT_SCN)) {
                if (!rs.next()) {
                    throw new IllegalStateException("Couldn't get SCN");
                }
                return rs.getLong(1);
            }
        }
    }

    public static void setSessionContainerIfCdbMode(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(LOG_MINER_SQL_IS_CDB)) {
                rs.next();
                // cdb模式 需要切换到根容器
                if (rs.getString(1).equalsIgnoreCase("YES")) {
                    try (PreparedStatement ps = connection.prepareStatement(LOG_MINER_SQL_ALTER_SESSION_CONTAINER)) {
                        try {
                            ps.execute();
                        } catch (SQLException e) {
                            throw new OracleException(String.format("sql=%s error=%s", LOG_MINER_SQL_ALTER_SESSION_CONTAINER, e.getMessage()));
                        }
                    }
                }
            }
        }
    }

    private static List<String> queryList(Connection connection, String querySql, String key) throws SQLException {
        List<String> list = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(querySql)) {
                while (rs.next()) {
                    String k = rs.getString(key);
                    if (StringUtil.isNotBlank(k)) {
                        list.add(k.toUpperCase());
                    }
                }
            }
        }
        return list;
    }

    public static void checkPermissions(Connection connection, int version) throws SQLException {
        List<String> roles = queryList(connection, LOG_MINER_SQL_QUERY_ROLES, LOG_MINER_KEY_GRANTED_ROLE);
        if (CollectionUtils.isEmpty(roles)) {
            throw new RuntimeException("No permissions");
        }

        // DBA
        if (roles.contains(LOG_MINER_DBA_ROLE)) {
            return;
        }

        List<String> privileges = queryList(connection, LOG_MINER_SQL_QUERY_PRIVILEGES, LOG_MINER_KEY_PRIVILEGE);
        if (CollectionUtils.isEmpty(privileges)) {
            throw new RuntimeException("No permissions");
        }
        List<String> checkPrivileges = version <= 11 ? LOG_MINER_ORACLE_11_PRIVILEGES_NEEDED : LOG_MINER_PRIVILEGES_NEEDED;
        long count = privileges.stream().filter(checkPrivileges::contains).count();
        if (count != checkPrivileges.size()) {
            String log = StringUtil.join(Collections.singleton(checkPrivileges), StringUtil.COMMA);
            throw new IllegalArgumentException(String.format("No permission, please execute sql authorization：GRANT %s TO USER_ROLE;", log));
        }
    }
}
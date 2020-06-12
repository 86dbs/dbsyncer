/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.listener.oracle.dcn;

import oracle.jdbc.OracleDriver;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.*;
import oracle.jdbc.driver.OracleConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 授予登录账号监听事件权限
 * <p>sqlplus/as sysdba
 * <p>
 * <p>grant change notification to AE86
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-08 21:53
 */
public class DBChangeNotification {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String QUERY_ROW_DATA_SQL  = "SELECT * FROM \"%s\" WHERE ROWID = \"%s\"";
    private static final String QUERY_TABLE_ALL_SQL = "SELECT DATA_OBJECT_ID, OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER='%S' AND OBJECT_TYPE = 'TABLE'";
    private static final String QUERY_TABLE_SQL     = "SELECT 1 FROM \"%s\" WHERE 1=2";
    private static final String QUERY_CALLBACK_SQL  = "SELECT REGID,CALLBACK FROM USER_CHANGE_NOTIFICATION_REGS";
    private static final String CALLBACK            = "net8://(ADDRESS=(PROTOCOL=tcp)(HOST=%s)(PORT=%s))?PR=0";
    private static String username;
    private static String password;
    private static String url;

    private OracleConnection           conn;
    private OracleStatement            statement;
    private DatabaseChangeRegistration dcr;
    private Map<Integer, String>       tables;

    public void start() throws SQLException {
        try {
            conn = connect();
            statement = (OracleStatement) conn.createStatement();
            readTables();

            Properties prop = new Properties();
            prop.setProperty(OracleConnection.DCN_NOTIFY_ROWIDS, "true");
            prop.setProperty(OracleConnection.DCN_IGNORE_UPDATEOP, "false");
            prop.setProperty(OracleConnection.DCN_IGNORE_INSERTOP, "false");
            prop.setProperty(OracleConnection.DCN_IGNORE_INSERTOP, "false");

            // add the listener:NTFDCNRegistration
            dcr = conn.registerDatabaseChangeNotification(prop);
            dcr.addListener(new DCNListener());

            final long regId = dcr.getRegId();
            final String host = getHost();
            final int port = getPort(dcr);
            final String callback = String.format(CALLBACK, host, port);
            logger.info("regId:{}, callback:{}", regId, callback);
            // clean the registrations
            clean(statement, regId, callback);
            statement.setDatabaseChangeRegistration(dcr);

            // 配置监听表
            for (Map.Entry<Integer, String> m : tables.entrySet()) {
                statement.executeQuery(String.format(QUERY_TABLE_SQL, m.getValue()));
            }
            String[] tableNames = dcr.getTables();
            int tableSize = tableNames.length;
            for (int i = 0; i < tableSize; i++) { logger.info("{} is part of the registration.", tableNames[i]); }
            logger.info("数据库更改通知开启");
        } catch (SQLException ex) {
            // if an exception occurs, we need to close the registration in order
            // to interrupt the thread otherwise it will be hanging around.
            close();
            throw ex;
        }
    }

    public void close() {
        try {
            if (null != statement) {
                statement.close();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }

        try {
            if (null != conn) {
                conn.unregisterDatabaseChangeNotification(dcr);
                conn.close();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
    }

    private void readTables() throws SQLException {
        tables = new LinkedHashMap<>();
        ResultSet rs = null;
        try {
            String sql = String.format(QUERY_TABLE_ALL_SQL, username);
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                int tableId = rs.getInt(1);
                String tableName = rs.getString(2);
                tables.put(tableId, tableName);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (null != rs) {
                rs.close();
            }
        }
    }

    private String getHost() {
        if (url != null) {
            String host = url.substring(url.indexOf("@") + 1);
            host = host.substring(0, host.indexOf(":"));
            return host;
        }
        return "127.0.0.1";
    }

    private int getPort(DatabaseChangeRegistration dcr) {
        Object obj = null;
        try {
            // 反射获取抽象属性 NTFRegistration
            Class clazz = dcr.getClass().getSuperclass();
            Method method = clazz.getDeclaredMethod("getClientTCPPort");
            method.setAccessible(true);
            obj = method.invoke(dcr, new Object[] {});
        } catch (NoSuchMethodException e) {
            logger.error(e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
        } catch (InvocationTargetException e) {
            logger.error(e.getMessage());
        }
        return null == obj ? 0 : Integer.parseInt(String.valueOf(obj));
    }

    private void clean(OracleStatement statement, long excludeRegId, String excludeCallback) throws SQLException {
        ResultSet rs = null;
        try {
            rs = statement.executeQuery(QUERY_CALLBACK_SQL);
            while (rs.next()) {
                long regId = rs.getLong(1);
                String callback = rs.getString(2);

                if (regId != excludeRegId && callback.equals(excludeCallback)) {
                    logger.info("Clean regid:{}, callback:{}", regId, callback);
                    conn.unregisterDatabaseChangeNotification(regId, callback);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (null != rs) {
                rs.close();
            }
        }
    }

    private OracleConnection connect() throws SQLException {
        // 加载连接参数
        username = "AE86".toUpperCase();
        password = "123".toUpperCase();
        url = "jdbc:oracle:thin:@127.0.0.1:1521:xe";

        OracleDriver dr = new OracleDriver();
        Properties prop = new Properties();
        prop.setProperty("user", username);
        prop.setProperty("password", password);
        return (OracleConnection) dr.connect(url, prop);
    }

    final class DCNListener implements DatabaseChangeListener {

        @Override
        public void onDatabaseChangeNotification(DatabaseChangeEvent event) {
            TableChangeDescription[] tds = event.getTableChangeDescription();
            logger.info("=============================");

            for (TableChangeDescription td : tds) {
                /**
                 * Select userenv('language') from dual;
                 * >SIMPLIFIED CHINESE_CHINA.AL32UTF8
                 *
                 * select * from V$NLS_PARAMETERS
                 * >AL32UTF8
                 *
                 */
                // oracle 11g XE 响应编码：AL32UTF8
                logger.info("数据库表id:{}", td.getObjectNumber());
                logger.info("数据表名称:{}", td.getTableName());
                logger.info("数据表名称:{}", tables.get(td.getObjectNumber()));

                // 获得返回的行级变化描述通知 行id、影响这一行的DML操作(行是插入、更新或删除的一种)
                RowChangeDescription[] rds = td.getRowChangeDescription();
                for (RowChangeDescription rd : rds) {
                    RowChangeDescription.RowOperation opr = rd.getRowOperation();
                    parseEvent(opr.name(), rd.getRowid().stringValue(), tables.get(td.getObjectNumber()));
                }
            }
        }

        private void parseEvent(String event, String rowId, String tableName){
            logger.info("event:{}, rowid:{}, tableName:{}", event, rowId, tableName);
            // QUERY_ROW_DATA_SQL

        }
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        DBChangeNotification client = new DBChangeNotification();
        client.start();
        // sleep 30s
        TimeUnit.SECONDS.sleep(120);
        client.close();
    }

}
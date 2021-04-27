/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.listener.oracle.dcn;

import oracle.jdbc.OracleDriver;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.*;
import oracle.jdbc.driver.OracleConnection;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

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

    private static final String QUERY_ROW_DATA_SQL  = "SELECT * FROM \"%s\" WHERE ROWID = '%s'";
    private static final String QUERY_TABLE_ALL_SQL = "SELECT DATA_OBJECT_ID, OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER='%S' AND OBJECT_TYPE = 'TABLE'";
    private static final String QUERY_TABLE_SQL     = "SELECT 1 FROM \"%s\" WHERE 1=2";
    private static final String QUERY_CALLBACK_SQL  = "SELECT REGID,CALLBACK FROM USER_CHANGE_NOTIFICATION_REGS";
    private static final String CALLBACK            = "net8://(ADDRESS=(PROTOCOL=tcp)(HOST=%s)(PORT=%s))?PR=0";

    private String                     username;
    private String                     password;
    private String                     url;
    private OracleConnection           conn;
    private OracleStatement            statement;
    private DatabaseChangeRegistration dcr;
    private Map<Integer, String>       tables;
    private List<RowEventListener>     listeners;

    public DBChangeNotification(String username, String password, String url) {
        this.username = username;
        this.password = password;
        this.url = url;
        this.listeners = new ArrayList<>();
    }

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

    private void close(ResultSet rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private void readTables() {
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
            close(rs);
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

    private void clean(OracleStatement statement, long excludeRegId, String excludeCallback) {
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
            close(rs);
        }
    }

    private OracleConnection connect() throws SQLException {
        OracleDriver dr = new OracleDriver();
        Properties prop = new Properties();
        prop.setProperty("user", username);
        prop.setProperty("password", password);
        return (OracleConnection) dr.connect(url, prop);
    }

    public void addRowEventListener(RowEventListener rowEventListener) {
        this.listeners.add(rowEventListener);
    }

    final class DCNListener implements DatabaseChangeListener {

        @Override
        public void onDatabaseChangeNotification(DatabaseChangeEvent event) {
            TableChangeDescription[] tds = event.getTableChangeDescription();

            for (TableChangeDescription td : tds) {
                RowChangeDescription[] rds = td.getRowChangeDescription();
                for (RowChangeDescription rd : rds) {
                    parseEvent(tables.get(td.getObjectNumber()), rd.getRowid().stringValue(), rd.getRowOperation());
                }
            }
        }

        private void parseEvent(String tableName, String rowId, RowChangeDescription.RowOperation event) {
            if (event.getCode() == TableChangeDescription.TableOperation.UPDATE.getCode()) {
                RowChangedEvent rowChangedEvent = new RowChangedEvent(tableName, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_LIST, read(tableName, rowId), rowId);
                listeners.forEach(e -> e.onEvents(rowChangedEvent));

            }else if(event.getCode() == TableChangeDescription.TableOperation.INSERT.getCode()){
                RowChangedEvent rowChangedEvent = new RowChangedEvent(tableName, ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_LIST, read(tableName, rowId), rowId);
                listeners.forEach(e -> e.onEvents(rowChangedEvent));

            }else{
                RowChangedEvent rowChangedEvent = new RowChangedEvent(tableName, ConnectorConstant.OPERTION_DELETE, Collections.EMPTY_LIST, Collections.EMPTY_LIST, rowId);
                listeners.forEach(e -> e.onEvents(rowChangedEvent));
            }
        }

        private List<Object> read(String tableName, String rowId) {
            List<Object> data = new ArrayList<>();
            ResultSet rs = null;
            try {
                rs = statement.executeQuery(String.format(QUERY_ROW_DATA_SQL, tableName, rowId));
                if(rs.next()){
                    final int size = rs.getMetaData().getColumnCount();
                    do{
                        data.add(rowId);
                        for (int i = 1; i <= size; i++) {
                            data.add(rs.getObject(i));
                        }
                    } while(rs.next());
                }
            } catch (SQLException e) {
                logger.error(e.getMessage());
            } finally {
                close(rs);
            }
            return data;
        }

    }

}
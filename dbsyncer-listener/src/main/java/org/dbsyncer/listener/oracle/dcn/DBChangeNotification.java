package org.dbsyncer.listener.oracle.dcn;

import oracle.jdbc.OracleDriver;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.*;
import oracle.jdbc.driver.OracleConnection;
import oracle.sql.ROWID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 授予登录账号监听事件权限
 * <p>grant change notification to AE86
 *
 * <p>select regid,callback from USER_CHANGE_NOTIFICATION_REGS
 * <p>select * from dba_change_notification_regs where username='AE86';
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-08 21:53
 */
public class DBChangeNotification {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static String username = "AE86";
    private static String password = "123";
    private static String url = "jdbc:oracle:thin:@127.0.0.1:1521:orcl";
    private static String host = "127.0.0.1";

    private OracleConnection conn;
    private DatabaseChangeRegistration dcr;

    public DBChangeNotification() throws SQLException {
        init();
    }

    public void init() throws SQLException {
        conn = connect();

        // 配置监听参数
        Properties prop = new Properties();
        prop.setProperty(OracleConnection.NTF_TIMEOUT, "0");
        prop.setProperty(OracleConnection.DCN_NOTIFY_ROWIDS, "true");
        prop.setProperty(OracleConnection.DCN_IGNORE_UPDATEOP, "false");
        prop.setProperty(OracleConnection.DCN_IGNORE_INSERTOP, "false");
        prop.setProperty(OracleConnection.DCN_IGNORE_INSERTOP, "false");

        try {
            // add the listener:NTFDCNRegistration
            dcr = conn.registerDatabaseChangeNotification(prop);

//            int port = getPort(dcr);
//            logger.info("当前regId：{}, port:{}", dcr.getRegId(), port);

            // 清空历史注册记录：
            // (ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=47632))?PR=0
//            cleanRegistrations(conn);

            dcr.addListener(new DCNListener());

            Statement stmt = conn.createStatement();
            // associate the statement with the registration:
            ((OracleStatement) stmt).setDatabaseChangeRegistration(dcr);
            // 配置监听表
            stmt.executeQuery("select * from \"my_user\" t where 1=2");
            stmt.executeQuery("select * from \"my_org\" t where 1=2");

            String[] tableNames = dcr.getTables();
            for (int i = 0; i < tableNames.length; i++)
                System.out.println(tableNames[i] + " is part of the registration.");

            stmt.close();
            logger.info("数据库更改通知开启");
        } catch (SQLException ex) {
            // if an exception occurs, we need to close the registration in order
            // to interrupt the thread otherwise it will be hanging around.
            if (conn != null) {
                conn.unregisterDatabaseChangeNotification(dcr);
            }
            throw ex;
        }
    }

    private int getPort(DatabaseChangeRegistration dcr) {
        Object obj = null;
        try {
            Method method = dcr.getClass().getMethod("getClientTCPPort");
            obj = method.invoke(dcr, new Object[]{});
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        return null == obj ? 0 : Integer.parseInt(String.valueOf(obj));
    }

    public void close() throws SQLException {
        if (null != conn) {
            conn.unregisterDatabaseChangeNotification(dcr);
            conn.close();
        }
    }

    /**
     * 清空历史注册
     *
     * @param conn
     * @throws SQLException
     */
    private void cleanRegistrations(OracleConnection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select regid,callback from USER_CHANGE_NOTIFICATION_REGS");
        while (rs.next()) {
            long regId = rs.getLong(1);
            String callback = rs.getString(2);
            logger.info("regid:{}, callback:{}", regId, callback);

            // TODO 排除当前注册
            conn.unregisterDatabaseChangeNotification(regId, callback);
        }
        rs.close();
        stmt.close();
    }

    /**
     * Creates a connection the database.
     */
    private OracleConnection connect() throws SQLException {
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
                logger.info("数据库表id:{}", td.getObjectNumber());
                logger.info("数据表名称:{}", td.getTableName());

                // 获得返回的行级变化描述通知 行id、影响这一行的DML操作(行是插入、更新或删除的一种)
                RowChangeDescription[] rds = td.getRowChangeDescription();
                for (RowChangeDescription rd : rds) {
                    RowChangeDescription.RowOperation rowOperation = rd.getRowOperation();
                    logger.info("数据库表行级变化：", rowOperation.toString());

                    ROWID rowid = rd.getRowid();
                    logger.info("事件：{}，ROWID：{}", rowOperation.name(), rowid.stringValue());
                }
            }
        }
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        DBChangeNotification client = new DBChangeNotification();
        // sleep 30s
        TimeUnit.SECONDS.sleep(120);
//        client.close();
    }

}
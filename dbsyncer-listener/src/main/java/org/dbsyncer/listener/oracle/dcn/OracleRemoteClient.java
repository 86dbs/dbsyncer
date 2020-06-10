package org.dbsyncer.listener.oracle.dcn;

import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.*;
import oracle.jdbc.driver.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import oracle.sql.ROWID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Properties;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-08 21:53
 */
public class OracleRemoteClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private OracleConnection conn;
    private OracleStatement statement;
    private DatabaseChangeRegistration dcr;

    public void init() throws SQLException, InterruptedException {
        OracleDataSource dataSource = new OracleDataSource();
        dataSource.setUser("ae86");
        dataSource.setPassword("123");
        dataSource.setURL("jdbc:oracle:thin:@127.0.0.1:1521:orcl");
        conn = (OracleConnection) dataSource.getConnection();

        // 配置监听参数
        Properties prop = new Properties();
        prop.setProperty(OracleConnection.NTF_TIMEOUT, "0");
        prop.setProperty(OracleConnection.DCN_NOTIFY_ROWIDS, "true");
        prop.setProperty(OracleConnection.DCN_IGNORE_UPDATEOP, "false");
        prop.setProperty(OracleConnection.DCN_IGNORE_INSERTOP, "false");
        prop.setProperty(OracleConnection.DCN_IGNORE_INSERTOP, "false");
        prop.setProperty(OracleConnection.CONNECTION_PROPERTY_CREATE_DESCRIPTOR_USE_CURRENT_SCHEMA_FOR_SCHEMA_NAME_DEFAULT, "true");

        statement = (OracleStatement) conn.createStatement();
        dcr = conn.registerDatabaseChangeNotification(prop);
        dcr.addListener(new DataBaseChangeListener());
        statement.setDatabaseChangeRegistration(dcr);

        // 监听的表
        statement.executeQuery("select * from \"my_user\" t where 1=2");
        statement.executeQuery("select * from \"my_org\" t where 1=2");

        long regId = dcr.getRegId();
        String[] dcrTables = dcr.getTables();
        logger.info("regId:{}", regId);
        logger.info("dcrTables:{}", dcrTables);

        logger.info("数据库更改通知开启");
    }

    public void close() throws SQLException {
        if (null != statement) {
            statement.close();
        }

        if (null != conn) {
            conn.unregisterDatabaseChangeNotification(dcr);
            conn.close();
        }
    }

    final class DataBaseChangeListener implements DatabaseChangeListener {

        @Override
        public void onDatabaseChangeNotification(DatabaseChangeEvent event) {
            TableChangeDescription[] tds = event.getTableChangeDescription();
            logger.info("=============================");

            for (TableChangeDescription td : tds) {
                EnumSet<TableChangeDescription.TableOperation> operations = td.getTableOperations();
                operations.contains("DELETE");
                logger.info("数据库表id：{}", td.getObjectNumber());
                logger.info("数据表名称：{}", td.getTableName());

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
        OracleRemoteClient client = new OracleRemoteClient();
        client.init();
        Thread.currentThread().join();
//        client.close();
    }

}

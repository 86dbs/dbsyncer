package org.dbsyncer.connector.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class JDBCUtil {

    private final static Logger logger = LoggerFactory.getLogger(JDBCUtil.class);

    public static Connection getConnection(String driver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        // com.mysql.jdbc.JDBC4Connection 
        // 不需要显式调用 Class.forName(driver), DriverManager.getConnection会自动加载合适的驱动
        return DriverManager.getConnection(url, username, password);
    }

    public static void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                logger.error(e.getClass() + " >> " + e.getLocalizedMessage());
            }
        }
    }

    public static void close(Connection conn) {
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getClass() + " >> " + e.getLocalizedMessage());
            }
        }
    }

    public static void close(Statement statement, Connection conn) {
        close(statement);
        close(conn);
    }

    //    public static void main(String[] args) {
    //        String url = "jdbc:mysql://10.238.206.222:13306/test?seUnicode=true&characterEncoding=UTF8&useSSL=true";
    //        String username = "root";
    //        String password = "123";
    //        Connection connection = JDBCUtil.getConnection("com.mysql.jdbc.Driver", url, username, password);
    //        if(connection!=null){
    //            JDBCUtil.close(connection);
    //        }else{
    //            logger.error("can not connect url.");
    //        }
    //    }

}

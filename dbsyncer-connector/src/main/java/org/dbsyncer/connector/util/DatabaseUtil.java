package org.dbsyncer.connector.util;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class DatabaseUtil {

    private DatabaseUtil() {
    }

    public static Connection getConnection(String driverClassName, String url, String username, String password) throws SQLException {
        if (StringUtil.isNotBlank(driverClassName)) {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                throw new ConnectorException(e.getCause());
            }
        }
        return DriverManager.getConnection(url, username, password);
    }

    public static void close(AutoCloseable rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
package org.dbsyncer.connector.util;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;

import static java.util.regex.Pattern.compile;

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

    public static String getDatabaseName(String url) {
        Matcher matcher = compile("(//)(?!(\\?)).+?(\\?)").matcher(url);
        while (matcher.find()) {
            url = matcher.group(0);
            break;
        }
        int s = url.lastIndexOf("/");
        int e = url.lastIndexOf("?");
        if (s > 0 && e > 0) {
            return StringUtil.substring(url, s + 1, e);
        }

        throw new ConnectorException("database is invalid");
    }

}
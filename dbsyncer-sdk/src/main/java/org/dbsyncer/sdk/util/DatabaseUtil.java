/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;

import static java.util.regex.Pattern.compile;

public abstract class DatabaseUtil {

    public static Connection getConnection(String driverClassName, String url, String username, String password) throws SQLException {
        if (StringUtil.isNotBlank(driverClassName)) {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                throw new SdkException(e.getCause());
            }
        }
        return DriverManager.getConnection(url, username, password);
    }

    public static Connection getConnection(String driverClassName, String url, Properties properties) throws SQLException {
        if (StringUtil.isNotBlank(driverClassName)) {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                throw new SdkException(e.getCause());
            }
        }
        return DriverManager.getConnection(url, properties);
    }

    public static void close(AutoCloseable rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (Exception e) {
                throw new SdkException(e);
            }
        }
    }

    public static String getDatabaseName(String url) {
        Matcher matcher = compile("(//)(?!(\\?)).+?(\\?)").matcher(url);
        if (matcher.find()) {
            url = matcher.group(0);
        }
        int s = url.lastIndexOf("/");
        int e = url.lastIndexOf("?");
        if (s > 0 && e > 0) {
            return StringUtil.substring(url, s + 1, e);
        }

        throw new SdkException("database is invalid");
    }

    public static Properties parseJdbcProperties(String params) {
        Properties properties = new Properties();
        if (params == null || params.trim().isEmpty()) {
            return properties;
        }
        // 按&分割参数
        String[] paramArray = params.split("&");
        for (String param : paramArray) {
            // 按=分割键值对
            String[] keyValue = param.split("=", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                if (!key.isEmpty()) {
                    properties.put(key, value);
                }
            }
        }
        return properties;
    }

}
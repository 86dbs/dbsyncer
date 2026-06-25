/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.starrocks.constant;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.constant.MySQLConstant;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;

import java.util.Properties;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 03:00
 */
public final class StarRocksConstant {

    public static final int DEFAULT_HTTP_PORT = 8030;

    public static final String HTTP_PORT = "httpPort";

    public static final String WRITE_MODE = "writeMode";

    public static final String WRITE_MODE_JDBC = "jdbc";

    public static final String WRITE_MODE_STREAM_LOAD = "stream_load";

    private StarRocksConstant() {
    }

    public static void enrichJdbcProperties(DatabaseConfig config) {
        MySQLConstant.enrichJdbcProperties(config);
        Properties properties = config.getProperties();
        if (properties == null) {
            properties = new Properties();
            config.setProperties(properties);
        }
        putIfAbsent(properties, "rewriteBatchedStatements", "true");
        putIfAbsent(properties, "useUnicode", "true");
        putIfAbsent(properties, "characterEncoding", "UTF-8");
        putIfAbsent(properties, "useSSL", "false");
    }

    public static void enrichExtInfo(DatabaseConfig config, String httpPort, String writeMode) {
        Properties extInfo = config.getExtInfo();
        if (extInfo == null) {
            extInfo = new Properties();
            config.setExtInfo(extInfo);
        }
        int port = NumberUtil.toInt(httpPort, DEFAULT_HTTP_PORT);
        if (port <= 0) {
            port = DEFAULT_HTTP_PORT;
        }
        extInfo.setProperty(HTTP_PORT, String.valueOf(port));
        if (StringUtil.isBlank(writeMode)) {
            writeMode = WRITE_MODE_JDBC;
        }
        extInfo.setProperty(WRITE_MODE, writeMode.trim());
    }

    public static int getHttpPort(DatabaseConfig config) {
        if (config == null || config.getExtInfo() == null) {
            return DEFAULT_HTTP_PORT;
        }
        return NumberUtil.toInt(config.getExtInfo().getProperty(HTTP_PORT), DEFAULT_HTTP_PORT);
    }

    public static boolean isStreamLoadMode(DatabaseConfig config) {
        if (config == null || config.getExtInfo() == null) {
            return false;
        }
        return StringUtil.equalsIgnoreCase(WRITE_MODE_STREAM_LOAD, config.getExtInfo().getProperty(WRITE_MODE));
    }

    public static boolean isStreamLoadEvent(String event) {
        return StringUtil.equals(event, ConnectorConstant.OPERTION_INSERT)
                || StringUtil.equals(event, ConnectorConstant.OPERTION_UPSERT)
                || StringUtil.equals(event, ConnectorConstant.OPERTION_UPDATE);
    }

    private static void putIfAbsent(Properties properties, String key, String value) {
        if (!properties.containsKey(key)) {
            properties.setProperty(key, value);
        }
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.doris.constant;

import org.dbsyncer.connector.mysql.constant.MySQLConstant;
import org.dbsyncer.sdk.config.DatabaseConfig;

import java.util.Properties;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 21:00
 */
public final class DorisConstant {

    private DorisConstant() {
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

    private static void putIfAbsent(Properties properties, String key, String value) {
        if (!properties.containsKey(key)) {
            properties.setProperty(key, value);
        }
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.constant;

import org.dbsyncer.sdk.config.DatabaseConfig;

import java.util.Properties;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 02:00
 */
public final class DamengConstant {

    private DamengConstant() {
    }

    public static void enrichJdbcProperties(DatabaseConfig config) {
        Properties properties = config.getProperties();
        if (properties == null) {
            properties = new Properties();
            config.setProperties(properties);
        }
        putIfAbsent(properties, "loginTimeout", "10");
        putIfAbsent(properties, "connectTimeout", "30000");
        putIfAbsent(properties, "socketTimeout", "600000");
    }

    private static void putIfAbsent(Properties properties, String key, String value) {
        if (!properties.containsKey(key)) {
            properties.setProperty(key, value);
        }
    }
}

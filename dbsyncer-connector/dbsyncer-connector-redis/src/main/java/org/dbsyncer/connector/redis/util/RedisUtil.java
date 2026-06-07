/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.redis.util;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.redis.config.RedisConfig;
import org.dbsyncer.sdk.util.PropertiesUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

/**
 * Redis连接器工具类
 */
public abstract class RedisUtil {

    public static final String DEFAULT_CONSUMER = "dbsyncer-consumer";

    public static JedisPool createPool(RedisConfig config) {
        String[] hostPort = parseHostPort(config.getUrl());
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        Properties props = config.getProperties();
        applyPoolConfig(poolConfig, props);

        int timeout = parseInt(props.getProperty("timeout"), 3000);
        String password = StringUtil.isBlank(config.getPassword()) ? null : config.getPassword();
        return new JedisPool(poolConfig, hostPort[0], Integer.parseInt(hostPort[1]), timeout, password, config.getDatabase());
    }

    public static String[] parseHostPort(String url) {
        String value = url == null ? "" : url.trim();
        if (value.startsWith("redis://")) {
            value = value.substring("redis://".length());
        }
        int slash = value.indexOf('/');
        if (slash > 0) {
            value = value.substring(0, slash);
        }
        String host = value;
        int port = 6379;
        int colon = value.lastIndexOf(':');
        if (colon > 0) {
            host = value.substring(0, colon);
            port = Integer.parseInt(value.substring(colon + 1));
        }
        return new String[]{host, String.valueOf(port)};
    }

    private static void applyPoolConfig(JedisPoolConfig poolConfig, Properties props) {
        poolConfig.setMaxTotal(parseInt(props.getProperty("maxTotal"), 32));
        poolConfig.setMaxIdle(parseInt(props.getProperty("maxIdle"), 16));
        poolConfig.setMinIdle(parseInt(props.getProperty("minIdle"), 2));
        poolConfig.setTestOnBorrow(parseBoolean(props.getProperty("testOnBorrow"), true));
        poolConfig.setTestWhileIdle(parseBoolean(props.getProperty("testWhileIdle"), true));
    }

    public static Properties parse(String properties) {
        properties = properties.replaceAll("\r\n", "&");
        properties = properties.replaceAll("\n", "&");
        return PropertiesUtil.parse(properties);
    }

    public static String toString(Properties properties) {
        String propertiesText = PropertiesUtil.toString(properties);
        return propertiesText.replaceAll("&", "\r\n");
    }

    public static void returnResource(JedisPool pool, Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    private static int parseInt(String value, int defaultValue) {
        if (StringUtil.isBlank(value)) {
            return defaultValue;
        }
        return Integer.parseInt(value.trim());
    }

    private static boolean parseBoolean(String value, boolean defaultValue) {
        if (StringUtil.isBlank(value)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.redis.config;

import org.dbsyncer.connector.redis.util.RedisUtil;
import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * Redis连接配置
 */
public class RedisConfig extends ConnectorConfig {

    private String url;

    private String password;

    private int database;

    @Override
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    @Override
    public String getPropertiesText() {
        return RedisUtil.toString(getProperties());
    }
}

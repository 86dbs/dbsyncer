/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb.config;

import org.dbsyncer.connector.mongodb.util.MongoUtil;
import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * MongoDB 连接配置
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public class MongoDBConfig extends ConnectorConfig {

    private String host;

    private int port = 27017;

    private String username;

    private String password;

    private String database;

    /**
     * 认证库，默认 admin
     */
    private String authDatabase = "admin";

    private int maxPoolSize = 64;

    private String url;

    @Override
    public String getUrl() {
        if (url == null) {
            url = MongoUtil.buildConnectionString(this);
        }
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getAuthDatabase() {
        return authDatabase;
    }

    public void setAuthDatabase(String authDatabase) {
        this.authDatabase = authDatabase;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }
}

/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.config;

import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.SqlTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @ClassName: DatabaseConfig
 * @Description: 数据库连接配置
 * @date: 2017年7月20日 下午3:40:59
 */
public class DatabaseConfig extends ConnectorConfig {

    /**
     * 驱动
     */
    private String driverClassName;

    /**
     * 连接地址
     */
    private String url;

    /**
     * 帐号
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 构架名
     */
    private String schema;

    /**
     * 最大连接数
     */
    private int maxActive = 128;

    /**
     * 连接有效期(ms)
     */
    private long keepAlive = 60000;

    /**
     * sql
     */
    private List<SqlTable> sqlTables;

    /**
     * 参数配置
     */
    private Map<String, String> properties = new ConcurrentHashMap<>();

    public String getProperty(String key) {
        return properties.get(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public int getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public long getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(long keepAlive) {
        this.keepAlive = keepAlive;
    }

    public List<SqlTable> getSqlTables() {
        return sqlTables;
    }

    public void setSqlTables(List<SqlTable> sqlTables) {
        this.sqlTables = sqlTables;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
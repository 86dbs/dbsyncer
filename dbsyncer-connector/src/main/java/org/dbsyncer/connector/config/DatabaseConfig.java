package org.dbsyncer.connector.config;

import org.dbsyncer.connector.model.SqlTable;
import org.dbsyncer.sdk.model.ConnectorConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     * sql
     */
    private List<SqlTable> sqlTables;

    /**
     * 参数配置
     */
    private Map<String, String> properties = new LinkedHashMap<>();

    public String getProperty(String key) {
        return properties.get(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.containsKey(key) ? properties.get(key) : defaultValue;
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
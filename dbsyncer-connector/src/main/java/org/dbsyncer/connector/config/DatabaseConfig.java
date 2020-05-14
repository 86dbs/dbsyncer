package org.dbsyncer.connector.config;

/**
 * @author AE86
 * @ClassName: DatabaseConfig
 * @Description: 数据库连接配置
 * @date: 2017年7月20日 下午3:40:59
 */
public class DatabaseConfig extends ConnectorConfig {

    // 驱动com.mysql.jdbc.Driver
    private String driverClassName;

    // 连接地址
    private String url;

    // 帐号
    private String username;

    // 密码
    private String password;

    // 通过SQL获取表信息
    private String sql;

    public String getDriverClassName() {
        return driverClassName;
    }

    public DatabaseConfig setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public DatabaseConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public DatabaseConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DatabaseConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

}
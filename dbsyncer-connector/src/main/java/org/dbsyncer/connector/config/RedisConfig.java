package org.dbsyncer.connector.config;

/**
 * @author AE86
 * @ClassName: RedisConfig
 * @Description: Redis连接配置
 * @date: 2018年5月23日 下午5:23:59
 */
public class RedisConfig extends ConnectorConfig {

    // 连接地址127.0.0.1:6379,127.0.0.1:6380
    private String url;

    // key
    private String key;

    // 密码
    private String password;

    public String getUrl() {
        return url;
    }

    public RedisConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getKey() {
        return key;
    }

    public RedisConfig setKey(String key) {
        this.key = key;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public RedisConfig setPassword(String password) {
        this.password = password;
        return this;
    }

}
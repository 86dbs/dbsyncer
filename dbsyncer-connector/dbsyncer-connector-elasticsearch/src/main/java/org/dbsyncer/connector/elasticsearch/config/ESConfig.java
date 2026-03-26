/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.config;

import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * ES连接配置
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-08-23 20:00
 */
public class ESConfig extends ConnectorConfig {

    /**
     * 集群地址, http(s)-9200, tcp-9300 http://192.168.1.100:9200,http://192.168.1.200:9200
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
     * 查询请求超时(秒)
     */
    private int timeoutSeconds = 10;

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

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }
}

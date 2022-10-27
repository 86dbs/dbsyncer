package org.dbsyncer.connector.config;

import org.dbsyncer.common.model.AbstractConnectorConfig;

/**
 * @author AE86
 * @ClassName: ESConfig
 * @Description: ES连接配置
 * @date: 2021年8月23日 下午8:00:00
 */
public class ESConfig extends AbstractConnectorConfig {

    /**
     * 集群地址192.168.1.100:9200,192.168.1.200:9200
     */
    private String url;

    /**
     * 协议9200(http),9300(tcp)
     */
    private String schema;

    /**
     * 帐号
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 索引(相当于数据库)
     */
    private String index;

    /**
     * 类型(相当于表)
     */
    private String type;

    /**
     * 主键
     */
    private String primaryKey;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
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

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }
}
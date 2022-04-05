package org.dbsyncer.connector.config;

/**
 * PostgreSQL连接配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/5 23:57
 */
public class PostgreSQLConfig extends DatabaseConfig {

    // 构架名
    private String schema;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
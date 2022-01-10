package org.dbsyncer.connector.config;

/**
 * SqlServer连接配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/1/10 23:57
 */
public class SqlServerDatabaseConfig extends DatabaseConfig {

    // 构架名
    private String schema;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}

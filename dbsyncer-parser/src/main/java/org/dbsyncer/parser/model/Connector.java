package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 12:40
 */
public class Connector extends ConfigModel {

    public Connector() {
        super.setType(ConfigConstant.CONNECTOR);
    }

    /**
     * 连接器配置
     */
    private ConnectorConfig config;

    /**
     * 数据库列表
     */
    private List<String> database;

    /**
     * schema列表
     */
    private List<String> schema;

    public ConnectorConfig getConfig() {
        return config;
    }

    public Connector setConfig(ConnectorConfig config) {
        this.config = config;
        return this;
    }

    public List<String> getDatabase() {
        return database;
    }

    public void setDatabase(List<String> database) {
        this.database = database;
    }

    public List<String> getSchema() {
        return schema;
    }

    public void setSchema(List<String> schema) {
        this.schema = schema;
    }
}

package org.dbsyncer.connector.config;

import java.util.List;
import java.util.Map;

public class WriterConfig {

    /**
     * 连接器配置
     */
    private ConnectorConfig     config;
    /**
     * 执行命令
     */
    private Map<String, String> command;
    /**
     * 字段信息
     */
    private List<Field>         fields;

    public ConnectorConfig getConfig() {
        return config;
    }

    public WriterConfig setConfig(ConnectorConfig config) {
        this.config = config;
        return this;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public WriterConfig setCommand(Map<String, String> command) {
        this.command = command;
        return this;
    }

    public List<Field> getFields() {
        return fields;
    }

    public WriterConfig setFields(List<Field> fields) {
        this.fields = fields;
        return this;
    }

}
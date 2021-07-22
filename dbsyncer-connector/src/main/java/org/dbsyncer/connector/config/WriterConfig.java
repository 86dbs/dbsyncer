package org.dbsyncer.connector.config;

import org.dbsyncer.connector.ConnectorMapper;

import java.util.List;
import java.util.Map;

public class WriterConfig {

    /**
     * 连接配置
     */
    private ConnectorMapper connectorMapper;
    /**
     * 执行命令
     */
    private Map<String, String> command;
    /**
     * 字段信息
     */
    private List<Field> fields;

    public ConnectorMapper getConnectorMapper() {
        return connectorMapper;
    }

    public WriterConfig setConnectorMapper(ConnectorMapper connectorMapper) {
        this.connectorMapper = connectorMapper;
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
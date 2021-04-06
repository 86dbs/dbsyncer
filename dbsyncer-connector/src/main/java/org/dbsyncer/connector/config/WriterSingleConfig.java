package org.dbsyncer.connector.config;

import java.util.List;
import java.util.Map;

public class WriterSingleConfig extends WriterConfig {

    /**
     * 行数据
     */
    private Map<String, Object> data;

    /**
     * 事件
     */
    private String event;

    private String table;

    public WriterSingleConfig(ConnectorConfig config, List<Field> fields, Map<String, String> command, String event, Map<String, Object> data, String table) {
        setConfig(config);
        setCommand(command);
        setFields(fields);
        setData(data);
        setEvent(event);
        setTable(table);
    }

    public Map<String, Object> getData() {
        return data;
    }

    public WriterSingleConfig setData(Map<String, Object> data) {
        this.data = data;
        return this;
    }

    public String getEvent() {
        return event;
    }

    public WriterSingleConfig setEvent(String event) {
        this.event = event;
        return this;
    }

    public String getTable() {
        return table;
    }

    public WriterSingleConfig setTable(String table) {
        this.table = table;
        return this;
    }
}
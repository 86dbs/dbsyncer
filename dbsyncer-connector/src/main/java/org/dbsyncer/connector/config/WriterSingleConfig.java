package org.dbsyncer.connector.config;

import org.dbsyncer.connector.ConnectorMapper;

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

    /**
     * 表名
     */
    private String table;

    /**
     * 更新失败转insert
     */
    private boolean retry;

    public WriterSingleConfig(ConnectorMapper connectorMapper, List<Field> fields, Map<String, String> command, String event, Map<String, Object> data, String table) {
        setConnectorMapper(connectorMapper);
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

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
    }
}
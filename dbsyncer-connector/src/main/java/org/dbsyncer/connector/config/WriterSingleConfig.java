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

    /**
     * 表名
     */
    private String table;

    /**
     * 重试标记
     */
    private boolean retry;

    /**
     * 更新失败转插入
     */
    private boolean forceUpdate;

    public WriterSingleConfig(List<Field> fields, Map<String, String> command, String event, Map<String, Object> data, String table, boolean forceUpdate) {
        setCommand(command);
        setFields(fields);
        setData(data);
        setEvent(event);
        setTable(table);
        this.forceUpdate = forceUpdate;
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

    public boolean isForceUpdate() {
        return forceUpdate;
    }

    public WriterSingleConfig setForceUpdate(boolean forceUpdate) {
        this.forceUpdate = forceUpdate;
        return this;
    }
}
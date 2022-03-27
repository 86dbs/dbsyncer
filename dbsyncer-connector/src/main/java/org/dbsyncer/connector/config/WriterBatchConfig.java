package org.dbsyncer.connector.config;

import java.util.List;
import java.util.Map;

public class WriterBatchConfig extends WriterConfig {

    /**
     * 集合数据
     */
    private List<Map> data;

    public WriterBatchConfig(String event, Map<String, String> command, List<Field> fields, List<Map> data) {
        setEvent(event);
        setCommand(command);
        setFields(fields);
        this.data = data;
    }

    public List<Map> getData() {
        return data;
    }

    public WriterBatchConfig setData(List<Map> data) {
        this.data = data;
        return this;
    }
}
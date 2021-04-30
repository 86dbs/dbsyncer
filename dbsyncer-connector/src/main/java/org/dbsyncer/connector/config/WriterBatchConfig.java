package org.dbsyncer.connector.config;

import java.util.List;
import java.util.Map;

public class WriterBatchConfig extends WriterConfig {

    /**
     * 集合数据
     */
    private List<Map> data;

    public WriterBatchConfig(ConnectorConfig config, Map<String, String> command, List<Field> fields, List<Map> data) {
        setConfig(config);
        setCommand(command);
        setFields(fields);
        setData(data);
    }

    public List<Map> getData() {
        return data;
    }

    public WriterBatchConfig setData(List<Map> data) {
        this.data = data;
        return this;
    }
}
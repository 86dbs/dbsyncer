package org.dbsyncer.sdk.config;

import org.dbsyncer.sdk.model.Field;

import java.util.List;
import java.util.Map;

public class WriterBatchConfig {

    /**
     * 表名
     */
    private final String tableName;
    /**
     * 事件
     */
    private final String event;
    /**
     * 执行命令
     */
    private final Map<String, String> command;
    /**
     * 字段信息
     */
    private final List<Field> fields;
    /**
     * 集合数据
     */
    private final List<Map> data;
    /**
     * 覆盖写入
     */
    private final boolean forceUpdate;
    /**
     * 是否启用字段解析器
     */
    private final boolean enableSchemaResolver;

    public WriterBatchConfig(String tableName, String event, Map<String, String> command, List<Field> fields, List<Map> data, boolean forceUpdate, boolean enableSchemaResolver) {
        this.tableName = tableName;
        this.event = event;
        this.command = command;
        this.fields = fields;
        this.data = data;
        this.forceUpdate = forceUpdate;
        this.enableSchemaResolver = enableSchemaResolver;
    }

    public String getTableName() {
        return tableName;
    }

    public String getEvent() {
        return event;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<Map> getData() {
        return data;
    }

    public boolean isForceUpdate() {
        return forceUpdate;
    }

    public boolean isEnableSchemaResolver() {
        return enableSchemaResolver;
    }
}
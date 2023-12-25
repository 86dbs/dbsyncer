package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Field;

import java.util.List;
import java.util.Map;

public final class BatchWriter {

    private ConnectorInstance connectorInstance;
    private Map<String, String> command;
    private String tableName;
    private String event;
    private List<Field> fields;
    private List<Map> dataList;
    private int batchSize;

    public BatchWriter(ConnectorInstance connectorInstance, Map<String, String> command, String tableName, String event,
                       List<Field> fields, List<Map> dataList, int batchSize) {
        this.connectorInstance = connectorInstance;
        this.command = command;
        this.tableName = tableName;
        this.event = event;
        this.fields = fields;
        this.dataList = dataList;
        this.batchSize = batchSize;
    }

    public ConnectorInstance getConnectorInstance() {
        return connectorInstance;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public String getTableName() {
        return tableName;
    }

    public String getEvent() {
        return event;
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<Map> getDataList() {
        return dataList;
    }

    public int getBatchSize() {
        return batchSize;
    }

}
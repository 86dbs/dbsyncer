package org.dbsyncer.parser.model;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.Field;

import java.util.List;
import java.util.Map;

public final class BatchWriter {

    private ConnectorMapper     connectorMapper;
    private Map<String, String> command;
    private String              event;
    private List<Field>         fields;
    private List<Map>           dataList;
    private int                 batchSize;
    private boolean             isForceUpdate;

    public BatchWriter(ConnectorMapper connectorMapper, Map<String, String> command, String event,
                       List<Field> fields, List<Map> dataList, int batchSize) {
        this(connectorMapper, command, event, fields, dataList, batchSize, false);
    }

    public BatchWriter(ConnectorMapper connectorMapper, Map<String, String> command, String event,
                       List<Field> fields, List<Map> dataList, int batchSize, boolean isForceUpdate) {
        this.connectorMapper = connectorMapper;
        this.command = command;
        this.event = event;
        this.fields = fields;
        this.dataList = dataList;
        this.batchSize = batchSize;
        this.isForceUpdate = isForceUpdate;
    }

    public ConnectorMapper getConnectorMapper() {
        return connectorMapper;
    }

    public Map<String, String> getCommand() {
        return command;
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

    public boolean isForceUpdate() {
        return isForceUpdate;
    }
}
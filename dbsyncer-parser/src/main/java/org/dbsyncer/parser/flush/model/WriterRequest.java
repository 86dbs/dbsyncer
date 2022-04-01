package org.dbsyncer.parser.flush.model;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.Field;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractRequest {

    private String metaId;

    private String tableGroupId;

    private String event;

    private ConnectorMapper connectorMapper;

    private List<Field> fields;

    private Map<String, String> command;

    private Map row;

    private boolean isForceUpdate;

    public WriterRequest(String metaId, String tableGroupId, String event, ConnectorMapper connectorMapper, List<Field> fields, Map<String, String> command, Map row, boolean isForceUpdate) {
        this.metaId = metaId;
        this.tableGroupId = tableGroupId;
        this.event = event;
        this.connectorMapper = connectorMapper;
        this.fields = fields;
        this.command = command;
        this.row = row;
        this.isForceUpdate = isForceUpdate;
    }

    public String getMetaId() {
        return metaId;
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public String getEvent() {
        return event;
    }

    public ConnectorMapper getConnectorMapper() {
        return connectorMapper;
    }

    public List<Field> getFields() {
        return fields;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public Map getRow() {
        return row;
    }

    public boolean isForceUpdate() {
        return isForceUpdate;
    }
}
package org.dbsyncer.parser.flush.model;

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

    private String targetConnectorId;

    private String tableGroupId;

    private String tableName;

    private String event;

    private List<Field> fields;

    private Map<String, String> command;

    private Map row;

    public WriterRequest(String metaId, String targetConnectorId, String tableGroupId, String tableName, String event, List<Field> fields,
                         Map<String, String> command, Map row) {
        this.metaId = metaId;
        this.targetConnectorId = targetConnectorId;
        this.tableGroupId = tableGroupId;
        this.tableName = tableName;
        this.event = event;
        this.fields = fields;
        this.command = command;
        this.row = row;
    }

    public String getMetaId() {
        return metaId;
    }

    public String getTargetConnectorId() {
        return targetConnectorId;
    }

    public String getTableGroupId() {
        return tableGroupId;
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

    public Map<String, String> getCommand() {
        return command;
    }

    public Map getRow() {
        return row;
    }

}
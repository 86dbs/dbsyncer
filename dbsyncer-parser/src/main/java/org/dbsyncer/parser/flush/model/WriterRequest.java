package org.dbsyncer.parser.flush.model;

import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.flush.BufferRequest;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractWriter implements BufferRequest {

    private String tableGroupId;

    private Map row;

    public WriterRequest(String tableGroupId, Map row, String metaId, String targetConnectorId, String sourceTableName, String targetTableName, String event, List<Field> fields, Map<String, String> command) {
        setMetaId(metaId);
        setTargetConnectorId(targetConnectorId);
        setSourceTableName(sourceTableName);
        setTargetTableName(targetTableName);
        setEvent(event);
        setFields(fields);
        setCommand(command);
        this.tableGroupId = tableGroupId;
        this.row = row;
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public Map getRow() {
        return row;
    }

}
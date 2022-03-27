package org.dbsyncer.parser.flush.model;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.parser.flush.AbstractFlushTask;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 18:11
 */
public class WriterFlushTask extends AbstractFlushTask {

    private boolean isMerged;

    private String metaId;

    private String event;

    private ConnectorMapper connectorMapper;

    private List<Field> fields;

    private Map<String, String> command;

    private List<Map> dataList = new LinkedList<>();

    public boolean isMerged() {
        return isMerged;
    }

    public void setMerged(boolean merged) {
        isMerged = merged;
    }

    public String getMetaId() {
        return metaId;
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public ConnectorMapper getConnectorMapper() {
        return connectorMapper;
    }

    public void setConnectorMapper(ConnectorMapper connectorMapper) {
        this.connectorMapper = connectorMapper;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public void setCommand(Map<String, String> command) {
        this.command = command;
    }

    public List<Map> getDataList() {
        return dataList;
    }

    public void setDataList(List<Map> dataList) {
        this.dataList = dataList;
    }

    @Override
    public int getFlushTaskSize() {
        return dataList.size();
    }

}
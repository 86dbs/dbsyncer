package org.dbsyncer.parser.model;

import org.dbsyncer.parser.flush.BufferRequest;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractWriter implements BufferRequest {

    private String messageId;

    private Map row;

    public WriterRequest(String tableGroupId, String event, Map row) {
        this(null, tableGroupId, event, row);
    }

    public WriterRequest(String messageId, String tableGroupId, String event, Map row) {
        setTableGroupId(tableGroupId);
        setEvent(event);
        this.messageId = messageId;
        this.row = row;
    }

    public String getMessageId() {
        return messageId;
    }

    public Map getRow() {
        return row;
    }

}
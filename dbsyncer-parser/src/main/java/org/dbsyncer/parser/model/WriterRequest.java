package org.dbsyncer.parser.model;

import org.dbsyncer.parser.flush.BufferRequest;
import org.dbsyncer.sdk.listener.ChangedEvent;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractWriter implements BufferRequest {

    private final List<Object> row;

    public WriterRequest(ChangedEvent event) {
        setTraceId(event.getTraceId());
        setTypeEnum(event.getType());
        setChangedOffset(event.getChangedOffset());
        setTableName(event.getSourceTableName());
        setEvent(event.getEvent());
        setSql(event.getSql());
        this.row = event.getChangedRow();
    }

    public List<Object> getRow() {
        return row;
    }

    @Override
    public String getTaskId() {
        return getChangedOffset().getTaskId();
    }
}

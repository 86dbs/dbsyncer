package org.dbsyncer.parser.model;

import org.dbsyncer.parser.flush.BufferRequest;
import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractWriter implements BufferRequest {

    private ChangedEventTypeEnum typeEnum;

    private Map row;

    private ChangedOffset changedOffset;

    private String sql;

    public WriterRequest(String tableGroupId, ChangedEvent event) {
        setTableGroupId(tableGroupId);
        setEvent(event.getEvent());
        this.typeEnum = event.getType();
        this.row = event.getChangedRow();
        this.changedOffset = event.getChangedOffset();
        this.sql = event.getSql();
    }

    @Override
    public String getMetaId() {
        return changedOffset.getMetaId();
    }

    public ChangedEventTypeEnum getTypeEnum() {
        return typeEnum;
    }

    public Map getRow() {
        return row;
    }

    public ChangedOffset getChangedOffset() {
        return changedOffset;
    }

    public String getSql() {
        return sql;
    }
}
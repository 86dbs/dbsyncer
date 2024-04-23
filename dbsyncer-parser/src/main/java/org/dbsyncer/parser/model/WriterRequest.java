package org.dbsyncer.parser.model;

import org.dbsyncer.parser.flush.BufferRequest;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractWriter implements BufferRequest {

    private Map row;

    private ChangedOffset changedOffset;

    public WriterRequest(String tableGroupId, ChangedEvent event) {
        setTypeEnum(event.getType());
        setTableGroupId(tableGroupId);
        setEvent(event.getEvent());
        setSql(event.getSql());
        this.row = event.getChangedRow();
        this.changedOffset = event.getChangedOffset();
    }

    @Override
    public String getMetaId() {
        return changedOffset.getMetaId();
    }

    public Map getRow() {
        return row;
    }

    public ChangedOffset getChangedOffset() {
        return changedOffset;
    }

}
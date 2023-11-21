package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.parser.flush.BufferRequest;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractWriter implements BufferRequest {

    private Map row;

    private ChangedOffset changedOffset;

    private String sql;

    public WriterRequest(String tableGroupId, ChangedEvent event) {
        setTableGroupId(tableGroupId);
        setEvent(event.getEvent());
        this.row = event.getChangedRow();
        this.changedOffset = event.getChangedOffset();
        if(event instanceof DDLChangedEvent){
            sql = ((DDLChangedEvent) event).getSql();
        }
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

    public String getSql() {
        return sql;
    }
}
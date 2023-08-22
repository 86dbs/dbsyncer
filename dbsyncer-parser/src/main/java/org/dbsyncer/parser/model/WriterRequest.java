package org.dbsyncer.parser.model;

import org.dbsyncer.parser.flush.BufferRequest;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractWriter implements BufferRequest {

    private Map row;

    public WriterRequest(String tableGroupId, String event, Map row) {
        setTableGroupId(tableGroupId);
        setEvent(event);
        this.row = row;
    }

    public Map getRow() {
        return row;
    }

}
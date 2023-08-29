package org.dbsyncer.parser.model;

import org.dbsyncer.parser.flush.BufferRequest;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class StorageRequest implements BufferRequest {

    private String metaId;

    private Map row;

    public StorageRequest(String metaId, Map row) {
        this.metaId = metaId;
        this.row = row;
    }

    @Override
    public String getMetaId() {
        return metaId;
    }

    public Map getRow() {
        return row;
    }
}
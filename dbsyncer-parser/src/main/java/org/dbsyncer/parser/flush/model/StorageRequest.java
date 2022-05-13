package org.dbsyncer.parser.flush.model;

import org.dbsyncer.parser.flush.BufferRequest;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class StorageRequest implements BufferRequest {

    private String metaId;

    private Map<String, Object> map;

    public StorageRequest(String metaId, Map<String, Object> map) {
        this.metaId = metaId;
        this.map = map;
    }

    public String getMetaId() {
        return metaId;
    }

    public Map<String, Object> getMap() {
        return map;
    }
}
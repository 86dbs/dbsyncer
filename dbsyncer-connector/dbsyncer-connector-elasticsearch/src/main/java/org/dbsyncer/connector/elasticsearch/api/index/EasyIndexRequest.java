/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.api.index;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

public class EasyIndexRequest extends IndexRequest implements RemoveTypeRequest {

    private boolean removeType;

    public EasyIndexRequest(String index, String type, boolean removeType, String id, Map data, XContentType xContentType) {
        super(index, type, id);
        source(data, xContentType);
        this.removeType = removeType;
    }

    @Override
    public boolean isRemoveType() {
        return removeType;
    }
}
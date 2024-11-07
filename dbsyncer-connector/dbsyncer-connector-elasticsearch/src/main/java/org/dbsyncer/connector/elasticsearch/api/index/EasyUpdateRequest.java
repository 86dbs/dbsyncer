/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.api.index;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

public class EasyUpdateRequest extends UpdateRequest implements RemoveTypeRequest {

    private boolean removeType;

    public EasyUpdateRequest(String index, String type, boolean removeType, String id, Map data, XContentType xContentType) {
        super(index, type, id);
        doc(data, xContentType);
        this.removeType = removeType;
    }

    @Override
    public boolean isRemoveType() {
        return removeType;
    }
}
/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.api.index;

import org.elasticsearch.action.delete.DeleteRequest;

public class EasyDeleteRequest extends DeleteRequest implements RemoveTypeRequest {
    private boolean removeType;

    public EasyDeleteRequest(String index, String type, boolean removeType, String id) {
        super(index, type, id);
        this.removeType = removeType;
    }

    @Override
    public boolean isRemoveType() {
        return removeType;
    }
}
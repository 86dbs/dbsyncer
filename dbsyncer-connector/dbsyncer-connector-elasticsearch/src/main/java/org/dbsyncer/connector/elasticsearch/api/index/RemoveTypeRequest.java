/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.api.index;

public interface RemoveTypeRequest {

    /**
     * 8.x 版本以上废弃Type
     *
     * @return
     */
    boolean isRemoveType();

}
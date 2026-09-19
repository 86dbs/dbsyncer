/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;

import java.util.Map;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-04 18:00
 */
public interface ValidateSyncDetailService {

    default Paging result(Map<String, String> params){
        return null;
    }

    default Map<String, Object> manualRevise(String taskId, String detailId){
        return null;
    }

    default void syncTaskTableMetaDetails(String taskId){

    }

    default void resetTaskDetailsForNewRound(String taskId){

    }

    default void markRunningDetailsDone(String taskId){

    }
}

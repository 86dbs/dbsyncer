/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;

import java.util.Map;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 13:46
 */
public interface DatabaseSyncDetailService {

    default Paging result(Map<String, String> params) {
        return null;
    }

    default void syncTaskTableMetaDetails(String taskId) {

    }

    default void resetTaskDetailsForNewRound(String taskId) {
    }

    default void markRunningDetailsDone(String taskId) {
    }
}

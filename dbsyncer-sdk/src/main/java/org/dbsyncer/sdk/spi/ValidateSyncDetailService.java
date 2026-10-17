/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import java.util.Map;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-04 18:00
 */
public interface ValidateSyncDetailService {

    default Map<String, Object> manualRevise(String taskId, String detailId) {
        return null;
    }

}

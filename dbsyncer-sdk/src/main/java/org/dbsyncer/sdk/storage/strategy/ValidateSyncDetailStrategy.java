/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2025-10-18 20:39
 */
public final class ValidateSyncDetailStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.VALIDATE_SYNC_DETAIL.getType();
    }
}

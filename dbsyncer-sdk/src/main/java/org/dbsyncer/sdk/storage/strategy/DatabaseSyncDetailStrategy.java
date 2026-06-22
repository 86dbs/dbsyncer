/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 15:00
 */
public final class DatabaseSyncDetailStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.DATABASE_SYNC_DETAIL.getType();
    }
}

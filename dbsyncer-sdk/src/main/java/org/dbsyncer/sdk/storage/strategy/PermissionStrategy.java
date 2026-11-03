/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 权限点：单表存储
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public final class PermissionStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.PERMISSION.getType();
    }
}

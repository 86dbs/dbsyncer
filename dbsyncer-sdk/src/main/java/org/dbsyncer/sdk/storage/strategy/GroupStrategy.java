/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 业务分组：单表存储
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
public final class GroupStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.GROUP.getType();
    }
}

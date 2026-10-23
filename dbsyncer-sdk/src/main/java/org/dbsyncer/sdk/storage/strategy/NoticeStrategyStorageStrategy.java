/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 告警策略：单表存储
 *
 * @author wuji
 * @version 1.0.0
 */
public final class NoticeStrategyStorageStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.NOTICE_STRATEGY.getType();
    }
}

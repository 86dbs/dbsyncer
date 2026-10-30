/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 同步任务 Meta 日汇总：单表存储
 *
 * @author wuji
 * @version 1.0.0
 */
public final class MetaAggStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.META_DAILY.getType();
    }
}

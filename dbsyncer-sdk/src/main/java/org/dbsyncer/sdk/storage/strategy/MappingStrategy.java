/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 历史 mapping 策略：已并入 {@link StorageEnum#TASK}，保留类名兼容调用方。
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-07-17 17:00
 */
public final class MappingStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.TASK.getType();
    }
}

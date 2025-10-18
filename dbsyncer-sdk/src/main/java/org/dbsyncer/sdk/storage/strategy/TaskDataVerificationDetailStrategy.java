/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * @Author wuji
 * @Version 1.0.0
 * @Date 2025-10-18 20:39
 */
public class TaskDataVerificationDetailStrategy implements Strategy {
    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.TASK_DATA_VERIFICATION_DETAIL.getType();
    }
}

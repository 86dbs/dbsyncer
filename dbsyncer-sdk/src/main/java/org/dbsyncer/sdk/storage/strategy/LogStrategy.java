/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 日志：Connector、Mapping、TableGroup、Meta、系统日志
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-16 23:22
 */
public final class LogStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.LOG.getType();
    }
}

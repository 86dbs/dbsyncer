/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

import org.springframework.util.Assert;

/**
 * 数据：全量或增量数据
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-16 23:22
 */
public final class DataStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        Assert.hasText(collectionId, "The collectionId is empty.");
        // 同步数据较多，根据不同的驱动生成集合ID: data/123
        return new StringBuilder(StorageEnum.DATA.getType()).append(separator).append(collectionId).toString();
    }
}

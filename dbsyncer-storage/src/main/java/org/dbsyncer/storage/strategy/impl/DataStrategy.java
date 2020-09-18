package org.dbsyncer.storage.strategy.impl;

import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.strategy.Strategy;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * 数据：全量或增量数据
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/15 22:40
 */
@Component
public class DataStrategy implements Strategy {

    @Override
    public String createCollectionId(String separator, String id) {
        Assert.hasText(id, "Id can not be empty.");
        // 同步数据较多，根据不同的驱动生成集合ID: data/123
        return new StringBuilder(StorageEnum.DATA.getType()).append(separator).append(id).toString();
    }
}
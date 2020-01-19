package org.dbsyncer.storage.strategy.impl;

import org.dbsyncer.storage.constant.StrategyConstant;
import org.dbsyncer.storage.strategy.Strategy;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.File;

/**
 * 数据：驱动实时同步数据
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/15 22:40
 */
@Component
public class DataStrategy implements Strategy {

    private static final String COLLECTION_ID = StrategyConstant.DATA + File.separator;

    @Override
    public String createCollectionId(String id) {
        Assert.hasText(id, "Id can not be empty.");
        // 同步数据较多，根据不同的驱动生成集合ID: /data/123
        return COLLECTION_ID + id;
    }
}
package org.dbsyncer.storage.strategy.impl;

import org.dbsyncer.storage.constant.StrategyConstant;
import org.dbsyncer.storage.strategy.Strategy;
import org.springframework.stereotype.Component;

/**
 * 配置：连接器、驱动、运行状态
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/15 22:39
 */
@Component
public class ConfigStrategy implements Strategy {

    private static final String COLLECTION_ID = StrategyConstant.CONFIG;

    @Override
    public String createCollectionId(String id) {
        return COLLECTION_ID;
    }
}
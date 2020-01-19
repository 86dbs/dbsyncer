package org.dbsyncer.storage.strategy.impl;

import org.dbsyncer.storage.constant.StrategyConstant;
import org.dbsyncer.storage.strategy.Strategy;
import org.springframework.stereotype.Component;

/**
 * 日志：连接器、驱动、系统
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/15 22:39
 */
@Component
public class LogStrategy implements Strategy {

    private static final String COLLECTION_ID = StrategyConstant.LOG;

    @Override
    public String createCollectionId(String id) {
        return COLLECTION_ID;
    }
}
package org.dbsyncer.storage.strategy.impl;

import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.strategy.Strategy;
import org.springframework.stereotype.Component;

/**
 * 日志：Connector、Mapping、TableGroup、Meta、系统日志
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/15 22:39
 */
@Component
public class LogStrategy implements Strategy {

    @Override
    public String createCollectionId(String separator, String id) {
        return StorageEnum.LOG.getType();
    }
}
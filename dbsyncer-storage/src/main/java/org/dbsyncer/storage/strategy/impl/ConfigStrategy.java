package org.dbsyncer.storage.strategy.impl;

import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.strategy.Strategy;
import org.springframework.stereotype.Component;

/**
 * 配置：Connector、Mapping、TableGroup、Meta、ProjectGroup、SystemConfig、UserConfig
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/15 22:39
 */
@Component
public class ConfigStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.CONFIG.getType();
    }
}
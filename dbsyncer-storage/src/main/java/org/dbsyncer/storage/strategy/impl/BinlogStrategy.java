package org.dbsyncer.storage.strategy.impl;

import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.strategy.Strategy;
import org.springframework.stereotype.Component;

/**
 * 缓存队列数据
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/15 22:39
 */
@Component
public class BinlogStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.BINLOG.getType();
    }
}
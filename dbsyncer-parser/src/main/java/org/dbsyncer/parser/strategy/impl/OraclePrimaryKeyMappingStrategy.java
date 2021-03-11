/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.parser.strategy.PrimaryKeyMappingStrategy;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/10/19 11:53
 */
public class OraclePrimaryKeyMappingStrategy implements PrimaryKeyMappingStrategy {

    /**
     * 替换主键
     *
     * @param row
     */
    @Override
    public void handle(Map row, RowChangedEvent rowChangedEvent) {
        row.put(rowChangedEvent.getPk(), rowChangedEvent.getRowId());
    }
}
/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.strategy;

import org.dbsyncer.common.event.RowChangedEvent;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/10/19 11:53
 */
public interface PrimaryKeyMappingStrategy {

    default void handle(Map row, RowChangedEvent rowChangedEvent) {
        // nothing to do
    }

}
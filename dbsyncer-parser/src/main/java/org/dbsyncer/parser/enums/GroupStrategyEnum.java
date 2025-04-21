/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.enums;

import org.dbsyncer.parser.strategy.GroupStrategy;
import org.dbsyncer.parser.strategy.impl.DefaultGroupStrategy;
import org.dbsyncer.parser.strategy.impl.PreloadTableGroupStrategy;
import org.dbsyncer.parser.strategy.impl.TableGroupStrategy;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum GroupStrategyEnum {

    /**
     * 默认
     */
    DEFAULT(new DefaultGroupStrategy()),
    /**
     * 预加载驱动表映射关系
     */
    PRELOAD_TABLE_GROUP(new PreloadTableGroupStrategy()),
    /**
     * 表映射关系
     */
    TABLE(new TableGroupStrategy());

    private final GroupStrategy groupStrategy;

    GroupStrategyEnum(GroupStrategy groupStrategy) {
        this.groupStrategy = groupStrategy;
    }

    public GroupStrategy getGroupStrategy() {
        return groupStrategy;
    }
}

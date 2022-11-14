package org.dbsyncer.manager.enums;

import org.dbsyncer.manager.GroupStrategy;
import org.dbsyncer.manager.strategy.DefaultGroupStrategy;
import org.dbsyncer.manager.strategy.TableGroupStrategy;

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
     * 表映射关系
     */
    TABLE(new TableGroupStrategy());

    private GroupStrategy groupStrategy;

    GroupStrategyEnum(GroupStrategy groupStrategy) {
        this.groupStrategy = groupStrategy;
    }

    public GroupStrategy getGroupStrategy() {
        return groupStrategy;
    }
}

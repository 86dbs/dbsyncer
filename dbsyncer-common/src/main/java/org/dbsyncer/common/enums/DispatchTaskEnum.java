/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

/**
 * 任务调度类型枚举
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-13 00:00
 */
public enum DispatchTaskEnum {

    /* 统计驱动总数 */
    MAPPING_COUNT("统计驱动总数"),

    /* 统计驱动表总数 */
    TABLE_GROUP_COUNT("统计驱动表总数");

    DispatchTaskEnum(String name) {
        this.name = name;
    }

    private final String name;

    public String getName() {
        return name;
    }
}

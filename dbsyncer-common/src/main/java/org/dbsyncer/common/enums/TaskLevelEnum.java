/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-24 10:35
 */
public enum TaskLevelEnum {
    /**
     * 0 任务级别（顶层任务）
     */
    TASK(0, "任务级别"),

    /**
     * 1 任务明细级别
     */
    TASK_DETAIL(1, "任务明细级别");

    private final int code;
    private final String desc;

    TaskLevelEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}

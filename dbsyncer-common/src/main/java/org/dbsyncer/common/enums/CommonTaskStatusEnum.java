/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

/**
 * 任务执行状态枚举
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-10-18 19:52
 */
public enum CommonTaskStatusEnum {

    /**
     * 未运行
     */
    READY(0, "未运行"),
    /**
     * 运行中
     */
    RUNNING(1, "运行中"),
    /**
     * 停止中
     */
    STOPPING(2, "停止中");

    private final int code;
    private final String message;

    CommonTaskStatusEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static boolean isRunning(int state) {
        return RUNNING.getCode() == state;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}

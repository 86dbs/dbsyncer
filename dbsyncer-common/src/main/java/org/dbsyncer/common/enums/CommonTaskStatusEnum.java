/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

/**
 * 通用任务状态（任务运行态与快照/进度态统一）：
 * 0-未运行；1-运行中；2-停止中；3-已完成。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-10-18 19:52
 */
public enum CommonTaskStatusEnum {

    /**
     * 未运行 / 未执行
     */
    READY(0, "未运行"),
    /**
     * 运行中
     */
    RUNNING(1, "运行中"),
    /**
     * 停止中
     */
    STOPPING(2, "停止中"),

    /**
     * 已完成
     */
    DONE(3, "已完成");

    private final int code;
    private final String message;

    CommonTaskStatusEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    /**
     * 运行中或停止中（尚未回到未运行/已完成）
     */
    public static boolean isRunning(int status) {
        return status == RUNNING.code || status == STOPPING.code;
    }

    public static boolean isStopping(int status) {
        return status == STOPPING.code;
    }

    public static boolean isDone(Integer status) {
        return status != null && status == DONE.code;
    }

    public static boolean isDone(int status) {
        return status == DONE.code;
    }

}

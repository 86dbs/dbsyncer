/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 通用任务子步状态（整库迁移 / 订正校验等快照共用）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 11:30
 */
public enum CommonTaskStepStatusEnum {

    /** 未完成 */
    PENDING(0, "未完成"),
    /** 已完成 */
    DONE(1, "已完成"),
    /** 失败 */
    FAILED(2, "失败");

    private final int code;
    private final String message;

    CommonTaskStepStatusEnum(int code, String message) {
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
     * 是否计入进度（已完成或跳过）。
     */
    public static boolean isDone(int status) {
        return status == DONE.code;
    }

    public static boolean isPending(int status) {
        return status == PENDING.code;
    }

    public static CommonTaskStepStatusEnum ofCode(int code) {
        for (CommonTaskStepStatusEnum value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return null;
    }
}

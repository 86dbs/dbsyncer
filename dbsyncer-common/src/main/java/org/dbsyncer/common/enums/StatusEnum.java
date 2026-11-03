/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

/**
 * 通用状态（整型落库：0-失败；1-成功）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-27
 */
public enum StatusEnum {

    /**
     * 失败
     */
    FAIL(0, "失败"),

    /**
     * 成功
     */
    SUCCESS(1, "成功");

    private final int code;
    private final String message;

    StatusEnum(int code, String message) {
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
     * 布尔 → 状态。
     *
     * @param success true 为成功
     * @return 成功/失败
     */
    public static StatusEnum of(boolean success) {
        return success ? SUCCESS : FAIL;
    }

    /**
     * 按 code 解析；未知值返回 null。
     *
     * @param code 状态码
     * @return 枚举或 null
     */
    public static StatusEnum get(Integer code) {
        if (code == null) {
            return null;
        }
        for (StatusEnum value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return null;
    }

    public static boolean isSuccess(Integer code) {
        return code != null && code == SUCCESS.code;
    }

    public static boolean isFail(Integer code) {
        return code != null && code == FAIL.code;
    }

}

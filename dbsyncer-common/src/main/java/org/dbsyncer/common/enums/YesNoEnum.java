/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

/**
 * 是/否（整型落库：1-是；0-否）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-29
 */
public enum YesNoEnum {

    /**
     * 否
     */
    NO(0, "否"),

    /**
     * 是
     */
    YES(1, "是");

    private final int code;
    private final String message;

    YesNoEnum(int code, String message) {
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
     * 布尔 → 枚举。
     *
     * @param value 布尔值
     * @return 是/否
     */
    public static YesNoEnum of(boolean value) {
        return value ? YES : NO;
    }

}

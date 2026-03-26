/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 版本类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/01/31 21:13
 */
public enum EditionEnum {

    /**
     * 社区版
     */
    COMMUNITY("community", "社区版"),

    /**
     * 专业版
     */
    PROFESSIONAL("professional", "专业版");

    private final String code;
    private final String message;

    EditionEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}

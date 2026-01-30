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
    COMMUNITY("community"),

    /**
     * 专业版
     */
    PROFESSIONAL("professional");

    private final String code;

    EditionEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

}
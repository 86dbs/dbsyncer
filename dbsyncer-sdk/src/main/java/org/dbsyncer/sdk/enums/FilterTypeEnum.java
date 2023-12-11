/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-11 20:29
 */
public enum FilterTypeEnum {
    /**
     * string
     */
    STRING("string"),
    /**
     * int
     */
    INT("int"),
    /**
     * long
     */
    LONG("long");

    private String type;

    FilterTypeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
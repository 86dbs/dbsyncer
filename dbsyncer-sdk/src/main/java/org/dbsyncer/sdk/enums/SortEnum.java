/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 支持的排序方式
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023/6/1 00:07
 */
public enum SortEnum {

    /**
     * 升序
     */
    ASC("asc"),
    /**
     * 降序
     */
    DESC("desc");

    SortEnum(String code) {
        this.code = code;
    }

    private final String code;

    public String getCode() {
        return code;
    }

    /**
     * 是否降序
     *
     * @return
     */
    public boolean isDesc() {
        return this == DESC;
    }
}

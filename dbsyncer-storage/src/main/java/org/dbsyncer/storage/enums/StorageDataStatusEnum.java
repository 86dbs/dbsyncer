/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.enums;

/**
 * 同步数据状态枚举
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-03-10 20:31
 */
public enum StorageDataStatusEnum {

    /**
     * 失败
     */
    FAIL(0),
    /**
     * 成功
     */
    SUCCESS(1);

    private Integer value;

    StorageDataStatusEnum(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }

}
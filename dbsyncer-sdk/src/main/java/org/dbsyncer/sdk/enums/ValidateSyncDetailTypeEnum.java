/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

import com.alibaba.fastjson2.annotation.JSONCreator;
import com.alibaba.fastjson2.annotation.JSONField;

/**
 * 订正校验明细类型（{@code dbsyncer_task_validate_sync_detail.type}）。
 *
 * @author wuji
 */
public enum ValidateSyncDetailTypeEnum {

    /**
     * 行数据校验
     */
    ROW_DATA("rowData"),
    /**
     * 表结构校验
     */
    TABLE_SCHEMA("tableSchema");

    private final String value;

    ValidateSyncDetailTypeEnum(String value) {
        this.value = value;
    }

    @JSONField
    public String getValue() {
        return value;
    }

    @JSONCreator
    public static ValidateSyncDetailTypeEnum fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (ValidateSyncDetailTypeEnum type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        return null;
    }
}

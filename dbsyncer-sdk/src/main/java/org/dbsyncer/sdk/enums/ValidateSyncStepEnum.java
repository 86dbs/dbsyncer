/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 订正校验表级快照阶段（写入 {@code CommonTaskSnapshot.step}）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-08 20:09
 */
public enum ValidateSyncStepEnum {
    TABLE_SCHEMA("tableSchema", "表结构"),
    TABLE_SCHEMA_SYNC("tableSchemaSync", "表结构订正"),
    ROW_DATA("rowData", "行数据"),
    ROW_DATA_SYNC("rowDataSync", "行数据订正"),
    ROW_DATA_REVERSE_SYNC("rowDataReverseSync", "行数据反向订正");

    private final String code;
    private final String message;

    ValidateSyncStepEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static ValidateSyncStepEnum ofCode(String code) {
        if (code == null) {
            return null;
        }
        for (ValidateSyncStepEnum value : values()) {
            if (value.code.equals(code)) {
                return value;
            }
        }
        return null;
    }
}

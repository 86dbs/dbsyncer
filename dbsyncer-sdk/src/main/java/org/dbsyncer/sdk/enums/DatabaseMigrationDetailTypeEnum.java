/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 整库迁移明细类型（{@code dbsyncer_task_database_sync_detail.TYPE}）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 16:00
 */
public enum DatabaseMigrationDetailTypeEnum {

    /** 结构迁移 */
    TABLE_SCHEMA("tableSchema"),
    /** 数据迁移 */
    ROW_DATA("rowData");

    private final String code;

    DatabaseMigrationDetailTypeEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static DatabaseMigrationDetailTypeEnum ofCode(String code) {
        if (code == null) {
            return null;
        }
        for (DatabaseMigrationDetailTypeEnum value : values()) {
            if (value.code.equals(code)) {
                return value;
            }
        }
        return null;
    }

    /**
     * 结构阶段是否已结束（含跳过）；已进入数据阶段视为结构已完成。
     */
    public boolean isSchemaPhaseDone(int status) {
        if (this == ROW_DATA) {
            return true;
        }
        return this == TABLE_SCHEMA && CommonTaskStepStatusEnum.isDone(status);
    }

    /**
     * 数据阶段是否已结束（含跳过）。
     */
    public boolean isDataPhaseDone(int status) {
        return this == ROW_DATA && CommonTaskStepStatusEnum.isDone(status);
    }

    /**
     * 根据阶段编码判断结构阶段是否已结束。
     */
    public static boolean isSchemaPhaseDone(String stepCode, int status) {
        DatabaseMigrationDetailTypeEnum step = ofCode(stepCode);
        return step != null && step.isSchemaPhaseDone(status);
    }

    /**
     * 根据阶段编码判断数据阶段是否已结束。
     */
    public static boolean isDataPhaseDone(String stepCode, int status) {
        DatabaseMigrationDetailTypeEnum step = ofCode(stepCode);
        return step != null && step.isDataPhaseDone(status);
    }
}

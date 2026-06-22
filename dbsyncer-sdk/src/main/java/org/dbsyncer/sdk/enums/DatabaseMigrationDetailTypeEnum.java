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
}

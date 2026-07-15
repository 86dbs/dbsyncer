/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.enums.DatabaseMigrationDetailTypeEnum;

/**
 * 整库迁移单表阶段终态结果
 *
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 16:00
 */
public class DatabaseMigrationDetailResult {

    private DatabaseMigrationDetailTypeEnum type;
    private int tableIndex;
    private String sourceDatabase;
    private String sourceSchema;
    private String targetDatabase;
    private String targetSchema;
    private String sourceTable;
    private String targetTable;
    private Long sourceTotal;
    private long successTotal;
    private long failTotal;
    private String content;

    public static DatabaseMigrationDetailResult of(DatabaseMigrationDetailTypeEnum type, int tableIndex) {
        DatabaseMigrationDetailResult result = new DatabaseMigrationDetailResult();
        result.type = type;
        result.tableIndex = tableIndex;
        return result;
    }

    public static DatabaseMigrationDetailResult schemaSuccess(int tableIndex) {
        return of(DatabaseMigrationDetailTypeEnum.TABLE_SCHEMA, tableIndex).success(1L);
    }

    public static DatabaseMigrationDetailResult schemaSkipped(int tableIndex) {
        return of(DatabaseMigrationDetailTypeEnum.TABLE_SCHEMA, tableIndex);
    }

    public static DatabaseMigrationDetailResult rowData(int tableIndex) {
        return of(DatabaseMigrationDetailTypeEnum.ROW_DATA, tableIndex);
    }

    public DatabaseMigrationDetailResult namespace(String sourceDatabase, String sourceSchema,
                                                   String targetDatabase, String targetSchema) {
        this.sourceDatabase = sourceDatabase;
        this.sourceSchema = sourceSchema;
        this.targetDatabase = targetDatabase;
        this.targetSchema = targetSchema;
        return this;
    }

    public DatabaseMigrationDetailResult tables(String sourceTable, String targetTable) {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        return this;
    }

    public DatabaseMigrationDetailResult sourceTotal(Long sourceTotal) {
        this.sourceTotal = sourceTotal;
        return this;
    }

    public DatabaseMigrationDetailResult success(long successTotal) {
        this.successTotal = successTotal;
        return this;
    }

    public DatabaseMigrationDetailResult fail(long failTotal) {
        this.failTotal = failTotal;
        return this;
    }

    public DatabaseMigrationDetailResult content(String content) {
        this.content = content;
        return this;
    }

    public DatabaseMigrationDetailTypeEnum getType() {
        return type;
    }

    public String getTypeCode() {
        return type == null ? null : type.getCode();
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public String getSourceSchema() {
        return sourceSchema;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public Long getSourceTotal() {
        return sourceTotal;
    }

    public long getSuccessTotal() {
        return successTotal;
    }

    public long getFailTotal() {
        return failTotal;
    }

    public String getContent() {
        return content;
    }
}

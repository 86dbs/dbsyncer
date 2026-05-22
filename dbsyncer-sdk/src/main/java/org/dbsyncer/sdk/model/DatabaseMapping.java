/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.List;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 11:14
 */
public class DatabaseMapping {

    /**
     * 源库名
     */
    private String sourceDatabase;

    /**
     * 目标库名
     */
    private String targetDatabase;

    /**
     * 表映射（源表 -> 目标表）
     */
    private List<TableMapping> tableMappings;

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public List<TableMapping> getTableMappings() {
        return tableMappings;
    }

    public void setTableMappings(List<TableMapping> tableMappings) {
        this.tableMappings = tableMappings;
    }

    public static class TableMapping {
        private String sourceTable;
        private String targetTable;

        public String getSourceTable() {
            return sourceTable;
        }

        public void setSourceTable(String sourceTable) {
            this.sourceTable = sourceTable;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(String targetTable) {
            this.targetTable = targetTable;
        }
    }
}
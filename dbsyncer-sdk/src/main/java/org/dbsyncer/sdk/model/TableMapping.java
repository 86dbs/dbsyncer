/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-08 20:47
 */
public class TableMapping {
    /**
     * 序号（从小到大，同一库映射内按此顺序处理）
     */
    private int index;

    private String sourceTable;
    private String targetTable;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

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

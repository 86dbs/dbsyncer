/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 库级映射配置
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 11:14
 */
public class DatabaseMapping {

    /**
     * 序号（从小到大，任务执行/恢复时按此顺序处理）
     */
    private int index;

    /**
     * 源库名
     */
    private String sourceDatabase;

    /**
     * 源 Schema
     */
    private String sourceSchema;

    /**
     * 源端连接器 ID
     */
    private String sourceConnectorId;

    /**
     * 目标端连接器 ID
     */
    private String targetConnectorId;

    /**
     * 目标库名
     */
    private String targetDatabase;

    /**
     * 目标 Schema（可选）
     */
    private String targetSchema;

    /**
     * 表映射（源表 -> 目标表）
     */
    private List<TableMapping> tableMappings;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public String getSourceConnectorId() {
        return sourceConnectorId;
    }

    public void setSourceConnectorId(String sourceConnectorId) {
        this.sourceConnectorId = sourceConnectorId;
    }

    public String getTargetConnectorId() {
        return targetConnectorId;
    }

    public void setTargetConnectorId(String targetConnectorId) {
        this.targetConnectorId = targetConnectorId;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema;
    }

    public List<TableMapping> getTableMappings() {
        return tableMappings;
    }

    public void setTableMappings(List<TableMapping> tableMappings) {
        this.tableMappings = tableMappings;
    }

    /**
     * 按 index 升序返回表映射列表（副本，不修改原列表）。
     */
    public List<TableMapping> getSortedTableMappings() {
        if (tableMappings == null || tableMappings.isEmpty()) {
            return Collections.emptyList();
        }
        return tableMappings.stream()
                .sorted(Comparator.comparingInt(TableMapping::getIndex))
                .collect(Collectors.toList());
    }

}

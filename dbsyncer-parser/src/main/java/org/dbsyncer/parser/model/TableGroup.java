/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表映射关系(dbsyncer_table_group)。
 * <p>关联信息落拆分列；字段映射/command/filter 等进 JSON。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/15 23:56
 */
public class TableGroup extends AbstractConfigModel {

    public TableGroup() {
        super.setType(ConfigConstant.TABLE_GROUP);
        super.setName(ConfigConstant.TABLE_GROUP);
    }

    /**
     * 排序号(SORT_INDEX)
     */
    private int index;

    /**
     * 任务ID
     */
    private String taskId;

    private String sourceConnectorId;
    private String targetConnectorId;
    private String sourceDatabase;
    private String targetDatabase;
    private String sourceSchema;
    private String targetSchema;

    /**
     * 数据源表(配置态完整 Table；名称同步到 SOURCE_TABLE 列)
     */
    private Table sourceTable;

    /**
     * 目标源表(配置态完整 Table；名称同步到 TARGET_TABLE 列)
     */
    private Table targetTable;

    private long sourceTotal;
    private long targetTotal;

    private List<FieldMapping> fieldMapping = new ArrayList<>();

    private Map<String, String> command = new HashMap<>();

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getTaskId() {
        return taskId;
    }

    public TableGroup setTaskId(String taskId) {
        this.taskId = taskId;
        return this;
    }

    /**
     * @deprecated 使用 {@link #getTaskId()}
     */
    @Deprecated
    public String getMappingId() {
        return taskId;
    }

    /**
     * @deprecated 使用 {@link #setTaskId(String)}
     */
    @Deprecated
    public TableGroup setMappingId(String mappingId) {
        this.taskId = mappingId;
        return this;
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

    public String getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public TableGroup setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public TableGroup setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
        return this;
    }

    public long getSourceTotal() {
        return sourceTotal;
    }

    public void setSourceTotal(long sourceTotal) {
        this.sourceTotal = sourceTotal;
    }

    public long getTargetTotal() {
        return targetTotal;
    }

    public void setTargetTotal(long targetTotal) {
        this.targetTotal = targetTotal;
    }

    public List<FieldMapping> getFieldMapping() {
        return fieldMapping;
    }

    public void setFieldMapping(List<FieldMapping> fieldMapping) {
        this.fieldMapping = fieldMapping;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public TableGroup setCommand(Map<String, String> command) {
        this.command = command;
        return this;
    }
}

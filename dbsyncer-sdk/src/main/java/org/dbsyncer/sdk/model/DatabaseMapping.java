/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.StringUtil;

/**
 * 库级映射配置（持久化于 task.JSON；不含表映射）。
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

    /**
     * 库维度聚合键：源/目标连接器 + 库 + schema（不含表名），与 TableGroup#buildDatabaseMappingKey 同规则。
     */
    public String buildDatabaseMappingKey() {
        return String.join("|",
                StringUtil.getIfBlank(sourceConnectorId, ""),
                StringUtil.getIfBlank(targetConnectorId, ""),
                StringUtil.getIfBlank(sourceDatabase, ""),
                StringUtil.getIfBlank(targetDatabase, ""),
                StringUtil.getIfBlank(sourceSchema, ""),
                StringUtil.getIfBlank(targetSchema, ""));
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.List;

/**
 * 整库迁移任务
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 11:19
 */
public class DatabaseSyncTask extends CommonTask {

    /** 源端连接器 ID */
    private String sourceConnectorId;
    /** 目标端连接器 ID */
    private String targetConnectorId;
    /** 库映射列表 */
    private List<DatabaseMapping> databaseMappings;

    /** 是否复制表结构 */
    private boolean enableCopySchema;
    /** 表结构是否覆盖（目标已存在时） */
    private boolean overwriteSchema;
    /** 是否同步数据 */
    private boolean enableCopyData;
    /** 数据是否覆盖（目标已存在时） */
    private boolean overwriteData;

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

    public List<DatabaseMapping> getDatabaseMappings() {
        return databaseMappings;
    }

    public void setDatabaseMappings(List<DatabaseMapping> databaseMappings) {
        this.databaseMappings = databaseMappings;
    }

    public boolean isEnableCopySchema() {
        return enableCopySchema;
    }

    public void setEnableCopySchema(boolean enableCopySchema) {
        this.enableCopySchema = enableCopySchema;
    }

    public boolean isOverwriteSchema() {
        return overwriteSchema;
    }

    public void setOverwriteSchema(boolean overwriteSchema) {
        this.overwriteSchema = overwriteSchema;
    }

    public boolean isEnableCopyData() {
        return enableCopyData;
    }

    public void setEnableCopyData(boolean enableCopyData) {
        this.enableCopyData = enableCopyData;
    }

    public boolean isOverwriteData() {
        return overwriteData;
    }

    public void setOverwriteData(boolean overwriteData) {
        this.overwriteData = overwriteData;
    }
}

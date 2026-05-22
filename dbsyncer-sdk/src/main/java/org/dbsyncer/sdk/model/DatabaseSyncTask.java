/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.List;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 11:19
 */
public class DatabaseSyncTask extends CommonTask {

    // 数据源连接器ID
    private String sourceConnectorId;
    // 目标连接器ID
    private String targetConnectorId;
    //数据库映射
    private List<DatabaseMapping> databaseMappings;

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
}
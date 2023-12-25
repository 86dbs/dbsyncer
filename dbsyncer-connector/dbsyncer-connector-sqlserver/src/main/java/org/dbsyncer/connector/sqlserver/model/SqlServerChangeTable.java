/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.common.util.JsonUtil;

public class SqlServerChangeTable {

    private String schemaName;
    private String tableName;
    private String captureInstance;
    private int changeTableObjectId;
    private byte[] startLsn;
    private byte[] stopLsn;
    private String capturedColumns;

    public SqlServerChangeTable(String schemaName, String tableName, String captureInstance, int changeTableObjectId, byte[] startLsn, byte[] stopLsn, String capturedColumns) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.captureInstance = captureInstance;
        this.capturedColumns = capturedColumns;
        this.changeTableObjectId = changeTableObjectId;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public int getChangeTableObjectId() {
        return changeTableObjectId;
    }

    public byte[] getStartLsn() {
        return startLsn;
    }

    public byte[] getStopLsn() {
        return stopLsn;
    }

    public String getCapturedColumns() {
        return capturedColumns;
    }

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }

}

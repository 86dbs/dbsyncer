package org.dbsyncer.listener.sqlserver;

import org.dbsyncer.common.util.JsonUtil;

public class SqlServerChangeTable {

    private String schemaName;
    private String tableName;
    private String captureInstance;
    private int changeTableObjectId;
    private byte[] startLsn;
    private byte[] stopLsn;
    private String capturedColumns;

    public SqlServerChangeTable(String schemaName, String tableName, String captureInstance,
                                int changeTableObjectId,
                                byte[] startLsn, byte[] stopLsn, String capturedColumns) {
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

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public void setCaptureInstance(String captureInstance) {
        this.captureInstance = captureInstance;
    }

    public int getChangeTableObjectId() {
        return changeTableObjectId;
    }

    public void setChangeTableObjectId(int changeTableObjectId) {
        this.changeTableObjectId = changeTableObjectId;
    }

    public byte[] getStartLsn() {
        return startLsn;
    }

    public void setStartLsn(byte[] startLsn) {
        this.startLsn = startLsn;
    }

    public byte[] getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(byte[] stopLsn) {
        this.stopLsn = stopLsn;
    }

    public String getCapturedColumns() {
        return capturedColumns;
    }

    public void setCapturedColumns(String capturedColumns) {
        this.capturedColumns = capturedColumns;
    }

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }

}

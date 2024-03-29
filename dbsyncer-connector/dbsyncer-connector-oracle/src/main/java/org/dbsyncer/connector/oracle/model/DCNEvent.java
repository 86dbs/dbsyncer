/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.model;

public final class DCNEvent {

    private String tableName;
    private String rowId;
    private int code;

    public DCNEvent(String tableName, String rowId, int code) {
        this.tableName = tableName;
        this.rowId = rowId;
        this.code = code;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRowId() {
        return rowId;
    }

    public int getCode() {
        return code;
    }
}
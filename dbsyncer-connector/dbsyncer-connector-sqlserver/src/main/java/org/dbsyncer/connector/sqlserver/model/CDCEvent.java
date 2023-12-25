/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.model;

import java.util.List;

public final class CDCEvent {

    private String tableName;
    private int code;
    private List<Object> row;

    public CDCEvent(String tableName, int code, List<Object> row) {
        this.tableName = tableName;
        this.code = code;
        this.row = row;
    }

    public String getTableName() {
        return tableName;
    }

    public int getCode() {
        return code;
    }

    public List<Object> getRow() {
        return row;
    }
}
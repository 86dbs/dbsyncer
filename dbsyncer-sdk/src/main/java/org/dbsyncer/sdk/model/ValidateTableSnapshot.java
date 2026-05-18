package org.dbsyncer.sdk.model;

import java.io.Serializable;

/**
 * 表校验快照
 *
 * @author wuji
 */
public class ValidateTableSnapshot implements Serializable {

    // 当前处理游标（偏移量/批次号）
    private long cursor;

    //是否完成 0 未完成
    private int status;

    public ValidateTableSnapshot(long cursor, int status) {
        this.cursor = cursor;
        this.status = status;
    }

    public long getCursor() {
        return cursor;
    }

    public void setCursor(long cursor) {
        this.cursor = cursor;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
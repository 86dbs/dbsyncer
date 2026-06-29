/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.io.Serializable;

/**
 * 表校验快照：正向与反向扫描各自维护续跑游标。
 *
 * @author wuji
 */
public class ValidateTableSnapshot implements Serializable {

    /** 正向扫描下一页页码 */
    private long cursor;

    /** 是否完成 0 未完成 1 已完成 */
    private int status;

    /** 正向扫描是否已完成 0 未完成 1 已完成 */
    private int sourceScanDone;

    /** 反向扫描下一页页码 */
    private long reverseCursor;

    public ValidateTableSnapshot(long cursor, int status) {
        this.cursor = cursor;
        this.status = status;
        this.sourceScanDone = 0;
        this.reverseCursor = 1L;
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

    public int getSourceScanDone() {
        return sourceScanDone;
    }

    public void setSourceScanDone(int sourceScanDone) {
        this.sourceScanDone = sourceScanDone;
    }

    public long getReverseCursor() {
        return reverseCursor;
    }

    public void setReverseCursor(long reverseCursor) {
        this.reverseCursor = reverseCursor;
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

/**
 * 全量同步单表进度（写入 Meta.snapshot.tableProgress）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-11
 */
public class TableSyncProgress {

    /**
     * 页码，从 1 开始
     */
    private int pageIndex = 1;

    /**
     * 游标（主键逗号拼接）
     */
    private String cursor = "";

    /**
     * 是否已完成
     */
    private boolean done;

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }
}

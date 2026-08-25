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

    /**
     * 写入时持有的派工 generation；用于拒绝过期写（非调度权威）。
     */
    private long generation;

    /**
     * 本工作项已读取/同步行数（游标分批 rowBudget 续跑用）。
     */
    private long processed;

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

    public long getGeneration() {
        return generation;
    }

    public void setGeneration(long generation) {
        this.generation = generation;
    }

    public long getProcessed() {
        return processed;
    }

    public void setProcessed(long processed) {
        this.processed = processed;
    }
}

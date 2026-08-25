/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

/**
 * 全量同步任务运行态。
 * 父任务负责启停；子任务（按表）承载独立游标,委托父任务。
 */
public class Task {

    private String id;

    private StateEnum state;

    private int pageIndex;

    private Object[] cursors;

    private long startTime;

    private long endTime;

    /**
     * 父任务（表级子任务时非空）
     */
    private Task parent;

    /**
     * 当前表映射 ID（表级子任务时非空）
     */
    private String tableGroupId;

    /**
     * 本工作项已处理行数（游标分批预算续跑）。
     */
    private long processed;

    public Task(String id) {
        this.id = id;
        this.state = StateEnum.RUNNING;
    }

    /**
     * 创建表级子任务：共享启停，独立游标。
     *
     * @param tableGroupId 表映射 ID
     * @return 子任务
     */
    public Task createTableTask(String tableGroupId) {
        Task child = new Task(this.id);
        child.parent = this;
        child.tableGroupId = tableGroupId;
        child.startTime = this.startTime;
        child.endTime = this.endTime;
        child.pageIndex = 1;
        return child;
    }

    public void stop() {
        this.state = StateEnum.STOP;
    }

    public boolean isRunning() {
        if (parent != null) {
            return parent.isRunning();
        }
        return StateEnum.RUNNING == state;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public Object[] getCursors() {
        return cursors;
    }

    public void setCursors(Object[] cursors) {
        this.cursors = cursors;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public Task getParent() {
        return parent;
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(String tableGroupId) {
        this.tableGroupId = tableGroupId;
    }

    public long getProcessed() {
        return processed;
    }

    public void setProcessed(long processed) {
        this.processed = processed;
    }

    public enum StateEnum {
        /**
         * 运行
         */
        RUNNING,
        /**
         * 停止
         */
        STOP;
    }
}

package org.dbsyncer.monitor.model;

public class AppReportMetric {

    /**
     * 已处理成功数
     */
    private long success;

    /**
     * 已处理失败数
     */
    private long fail;

    /**
     * 插入事件
     */
    private long insert;

    /**
     * 更新事件
     */
    private long update;

    /**
     * 删除事件
     */
    private long delete;

    /**
     * 待处理数
     */
    private long queueUp;

    /**
     * 队列长度
     */
    private long queueCapacity;

    public long getSuccess() {
        return success;
    }

    public void setSuccess(long success) {
        this.success = success;
    }

    public long getFail() {
        return fail;
    }

    public void setFail(long fail) {
        this.fail = fail;
    }

    public long getInsert() {
        return insert;
    }

    public void setInsert(long insert) {
        this.insert = insert;
    }

    public long getUpdate() {
        return update;
    }

    public void setUpdate(long update) {
        this.update = update;
    }

    public long getDelete() {
        return delete;
    }

    public void setDelete(long delete) {
        this.delete = delete;
    }

    public long getQueueUp() {
        return queueUp;
    }

    public void setQueueUp(long queueUp) {
        this.queueUp = queueUp;
    }

    public long getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(long queueCapacity) {
        this.queueCapacity = queueCapacity;
    }
}
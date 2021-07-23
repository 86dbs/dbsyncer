package org.dbsyncer.biz.vo;

public class AppReportMetricVo {

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
     * 待处理
     */
    private long submittedTask;

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

    public long getSubmittedTask() {
        return submittedTask;
    }

    public void setSubmittedTask(long submittedTask) {
        this.submittedTask = submittedTask;
    }
}
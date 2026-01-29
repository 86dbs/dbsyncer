package org.dbsyncer.biz.model;

public class MappingReportMetric {

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
     * ddl事件
     */
    private long ddl;

    /**
     * 昨天同步总数（成功+失败）
     */
    private long yesterdayData;

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

    public long getDdl() {
        return ddl;
    }

    public void setDdl(long ddl) {
        this.ddl = ddl;
    }

    public long getYesterdayData() {
        return yesterdayData;
    }

    public void setYesterdayData(long yesterdayData) {
        this.yesterdayData = yesterdayData;
    }

    public void reset() {
        this.success = 0;
        this.fail = 0;
        this.insert = 0;
        this.update = 0;
        this.delete = 0;
        this.ddl = 0;
        this.yesterdayData = 0;
    }
}
/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.model;

import org.dbsyncer.biz.vo.SyncTrendStackVO;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-12-16 23:59
 */
public final class DashboardMetric extends MappingReportMetric {

    /**
     * 总任务数
     */
    private long totalMeta;

    /**
     * 上周任务数
     */
    private long lastWeekMeta;

    /**
     * 运行任务数
     */
    private long runningMeta;

    /**
     * 失败任务数
     */
    private long failMeta;

    /**
     * 同步趋势数据
     */
    private SyncTrendStackVO trend = new SyncTrendStackVO();

    public long getTotalMeta() {
        return totalMeta;
    }

    public void setTotalMeta(long totalMeta) {
        this.totalMeta = totalMeta;
    }

    public long getLastWeekMeta() {
        return lastWeekMeta;
    }

    public void setLastWeekMeta(long lastWeekMeta) {
        this.lastWeekMeta = lastWeekMeta;
    }

    public long getRunningMeta() {
        return runningMeta;
    }

    public void setRunningMeta(long runningMeta) {
        this.runningMeta = runningMeta;
    }

    public long getFailMeta() {
        return failMeta;
    }

    public void setFailMeta(long failMeta) {
        this.failMeta = failMeta;
    }

    public SyncTrendStackVO getTrend() {
        return trend;
    }

    public void setTrend(SyncTrendStackVO trend) {
        this.trend = trend;
    }

    public void reset() {
        super.reset();
        this.totalMeta = 0;
        this.lastWeekMeta = 0;
        this.runningMeta = 0;
        this.failMeta = 0;
        trend = new SyncTrendStackVO();
    }
}

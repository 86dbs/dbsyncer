/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.model.ValidateSyncTask;

import java.math.BigDecimal;

public final class ValidateSyncTaskVO extends ValidateSyncTask {
    // 连接器
    private final Connector sourceConnector;
    private final Connector targetConnector;
    // 错误数（任务级 Meta 累计差异行数）
    private long errorCount;
    //当前进度
    private BigDecimal progress;
    // 表总数
    private int totalTableCount;
    // 已完成表数
    private int completedTableCount;
    /** 任务级 Meta.state（本轮业务态，含 DONE=3） */
    private Integer metaState;
    /** 本轮执行开始时间（任务级 Meta） */
    private Long startTime;
    /** 本轮执行结束时间（任务级 Meta） */
    private Long endTime;

    public ValidateSyncTaskVO(Connector sourceConnector, Connector targetConnector) {
        this.sourceConnector = sourceConnector;
        this.targetConnector = targetConnector;
    }

    public Connector getSourceConnector() {
        return sourceConnector;
    }

    public Connector getTargetConnector() {
        return targetConnector;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    public BigDecimal getProgress() {
        return progress;
    }

    public void setProgress(BigDecimal progress) {
        this.progress = progress;
    }

    public int getTotalTableCount() {
        return totalTableCount;
    }

    public void setTotalTableCount(int totalTableCount) {
        this.totalTableCount = totalTableCount;
    }

    public int getCompletedTableCount() {
        return completedTableCount;
    }

    public void setCompletedTableCount(int completedTableCount) {
        this.completedTableCount = completedTableCount;
    }

    public Integer getMetaState() {
        return metaState;
    }

    public void setMetaState(Integer metaState) {
        this.metaState = metaState;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }
}

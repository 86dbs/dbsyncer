/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.DatabaseSyncTask;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 整库迁移任务列表/详情 VO。
 * <p>接口出入参中的 {@code databaseMappings} 为 {@link DatabaseMappingVO}（含表）；
 * 任务持久化库映射仍写在父类字段（仅库维）。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 00:00
 */
public final class DatabaseSyncTaskVO extends DatabaseSyncTask {

    private final Connector sourceConnector;
    private final Connector targetConnector;

    /**
     * 编辑/展示用库映射（含表）
     */
    private List<DatabaseMappingVO> mappingViews;

    private int mappingCount;
    /** 任务进度 0~100，由任务/明细 Meta 计算 */
    private BigDecimal progress;
    /** 失败数（任务级 Meta 累计 FAIL） */
    private long errorCount;
    /** 任务总表数（TableGroup 数量） */
    private int totalTableCount;
    /** 已完成表数（由 Meta 进度换算） */
    private int completedTableCount;
    /** 任务级 Meta.state（本轮业务态，含 DONE=3） */
    private Integer metaState;
    /** 本轮执行开始时间（任务级 Meta） */
    private Long beginTime;
    /** 本轮执行结束时间（任务级 Meta） */
    private Long endTime;

    public DatabaseSyncTaskVO(Connector sourceConnector, Connector targetConnector) {
        this.sourceConnector = sourceConnector;
        this.targetConnector = targetConnector;
    }

    public Connector getSourceConnector() {
        return sourceConnector;
    }

    public Connector getTargetConnector() {
        return targetConnector;
    }

    public List<DatabaseMappingVO> getMappingViews() {
        return mappingViews;
    }

    public void setMappingViews(List<DatabaseMappingVO> mappingViews) {
        this.mappingViews = mappingViews;
    }

    /**
     * 序列化为前端字段 {@code databaseMappings}（元素运行时为 {@link DatabaseMappingVO}）。
     */
    @Override
    public List<DatabaseMapping> getDatabaseMappings() {
        if (mappingViews == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(mappingViews);
    }

    /**
     * BeanUtils / 接口反序列化入口；若元素已是 VO 则保留表映射。
     */
    @Override
    public void setDatabaseMappings(List<DatabaseMapping> databaseMappings) {
        if (databaseMappings == null) {
            this.mappingViews = null;
            return;
        }
        List<DatabaseMappingVO> views = new ArrayList<>(databaseMappings.size());
        for (DatabaseMapping mapping : databaseMappings) {
            if (mapping instanceof DatabaseMappingVO) {
                views.add((DatabaseMappingVO) mapping);
            } else {
                views.add(DatabaseMappingVO.from(mapping));
            }
        }
        this.mappingViews = views;
    }

    public int getMappingCount() {
        return mappingCount;
    }

    public void setMappingCount(int mappingCount) {
        this.mappingCount = mappingCount;
    }

    public BigDecimal getProgress() {
        return progress;
    }

    public void setProgress(BigDecimal progress) {
        this.progress = progress;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
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

    public Long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(Long beginTime) {
        this.beginTime = beginTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }
}

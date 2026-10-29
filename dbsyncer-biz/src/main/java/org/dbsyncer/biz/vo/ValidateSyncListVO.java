/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.math.BigDecimal;

/**
 * 订正校验列表行（仅列表展示字段，不含完整配置）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-29
 */
public class ValidateSyncListVO {

    private String id;
    private String name;
    private String trigger;
    private MappingListConnectorVO sourceConnector;
    private MappingListConnectorVO targetConnector;
    private long errorCount;
    private BigDecimal progress;
    private int totalTableCount;
    private int completedTableCount;
    private Integer metaState;
    private Long startTime;
    private Long updateTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTrigger() {
        return trigger;
    }

    public void setTrigger(String trigger) {
        this.trigger = trigger;
    }

    public MappingListConnectorVO getSourceConnector() {
        return sourceConnector;
    }

    public void setSourceConnector(MappingListConnectorVO sourceConnector) {
        this.sourceConnector = sourceConnector;
    }

    public MappingListConnectorVO getTargetConnector() {
        return targetConnector;
    }

    public void setTargetConnector(MappingListConnectorVO targetConnector) {
        this.targetConnector = targetConnector;
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

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }
}

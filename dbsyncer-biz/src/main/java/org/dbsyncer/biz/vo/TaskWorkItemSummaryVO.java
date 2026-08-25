/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

/**
 * 任务工作项汇总（Leader 内存 Assignment 视图）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-19
 */
public class TaskWorkItemSummaryVO {

    private String taskId;
    private int workItemCount;
    /**
     * 节点分布，如 nodeA:3, nodeB:2
     */
    private String nodeDistribution;
    private long maxGeneration;
    /**
     * true 表示整增量任务级派工（itemId=taskId），计入增量任务数而非全量工作项数。
     */
    private boolean incrementTask;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public int getWorkItemCount() {
        return workItemCount;
    }

    public void setWorkItemCount(int workItemCount) {
        this.workItemCount = workItemCount;
    }

    public String getNodeDistribution() {
        return nodeDistribution;
    }

    public void setNodeDistribution(String nodeDistribution) {
        this.nodeDistribution = nodeDistribution;
    }

    public long getMaxGeneration() {
        return maxGeneration;
    }

    public void setMaxGeneration(long maxGeneration) {
        this.maxGeneration = maxGeneration;
    }

    public boolean isIncrementTask() {
        return incrementTask;
    }

    public void setIncrementTask(boolean incrementTask) {
        this.incrementTask = incrementTask;
    }
}

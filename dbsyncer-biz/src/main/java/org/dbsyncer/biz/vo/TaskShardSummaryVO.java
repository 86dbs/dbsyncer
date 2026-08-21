/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

/**
 * 任务分片汇总（Leader 内存 Assignment 视图）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-19
 */
public class TaskShardSummaryVO {

    private String taskId;
    private int shardCount;
    /**
     * 节点分布，如 nodeA:3, nodeB:2
     */
    private String nodeDistribution;
    private long maxGeneration;
    /**
     * true 表示整增量任务级派工（itemId=taskId），计入增量任务数而非全量分片数。
     */
    private boolean incrementTask;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
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

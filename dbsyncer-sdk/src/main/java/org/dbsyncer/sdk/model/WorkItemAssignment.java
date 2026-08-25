/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

/**
 * Leader 内存派工视图中的一条工作项分配（非持久化权威）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-19
 */
public class WorkItemAssignment {

    /**
     * 所属任务 ID（Mapping / Validate / Migration）。
     */
    private String taskId;
    /**
     * 工作项 ID（Phase1 为 tableGroupId）。
     */
    private String itemId;
    /**
     * 执行节点 ID。
     */
    private String nodeId;
    /**
     * 围栏代数，写前必须匹配。
     */
    private long generation;

    public WorkItemAssignment() {
    }

    public WorkItemAssignment(String taskId, String itemId, String nodeId, long generation) {
        this.taskId = taskId;
        this.itemId = itemId;
        this.nodeId = nodeId;
        this.generation = generation;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public long getGeneration() {
        return generation;
    }

    public void setGeneration(long generation) {
        this.generation = generation;
    }
}

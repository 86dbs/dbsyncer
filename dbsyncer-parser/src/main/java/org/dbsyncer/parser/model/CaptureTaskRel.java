/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

/**
 * 远端采集任务与同步任务关联（对应 {@code dbsyncer_capture_task_rel}）。
 * <p>{@code captureTaskId} 指向 {@code dbsyncer_task.ID}（TYPE=captureRemote）。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
public class CaptureTaskRel {

    private String id;
    private String captureTaskId;
    private String taskId;
    private Long createTime;
    private Long updateTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCaptureTaskId() {
        return captureTaskId;
    }

    public void setCaptureTaskId(String captureTaskId) {
        this.captureTaskId = captureTaskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }
}

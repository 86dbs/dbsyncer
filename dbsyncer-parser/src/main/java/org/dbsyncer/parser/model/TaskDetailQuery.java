/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.parser.enums.TaskDetailMetricEnum;
import org.dbsyncer.parser.enums.TaskDetailOrderEnum;

/**
 * 任务明细连表查询参数。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 16:00
 */
public class TaskDetailQuery {

    private String taskId;
    private int pageNum = 1;
    private int pageSize = 10;
    private String detailType;
    private String detailStatus;
    private TaskDetailMetricEnum metric;
    private TaskDetailOrderEnum orderBy;

    private TaskDetailQuery() {
    }

    /**
     * 按任务 ID 构建查询。
     */
    public static TaskDetailQuery of(String taskId) {
        TaskDetailQuery query = new TaskDetailQuery();
        query.taskId = taskId;
        return query;
    }

    public TaskDetailQuery page(int pageNum, int pageSize) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        return this;
    }

    public TaskDetailQuery detailType(String detailType) {
        this.detailType = detailType;
        return this;
    }

    public TaskDetailQuery detailStatus(String detailStatus) {
        this.detailStatus = detailStatus;
        return this;
    }

    public TaskDetailQuery metric(TaskDetailMetricEnum metric) {
        this.metric = metric;
        return this;
    }

    public TaskDetailQuery orderBy(TaskDetailOrderEnum orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getPageNum() {
        return pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public String getDetailType() {
        return detailType;
    }

    public String getDetailStatus() {
        return detailStatus;
    }

    public TaskDetailMetricEnum getMetric() {
        return metric;
    }

    public TaskDetailOrderEnum getOrderBy() {
        return orderBy;
    }
}

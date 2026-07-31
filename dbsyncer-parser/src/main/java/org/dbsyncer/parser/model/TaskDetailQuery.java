/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.parser.enums.TaskDetailMetricEnum;
import org.dbsyncer.parser.enums.TaskDetailOrderEnum;

/**
 * 任务明细查询参数。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 16:00
 */
public class TaskDetailQuery {

    private String taskId;
    private String detailId;
    private int pageNum = 1;
    private int pageSize = 10;
    private String detailType;
    private String detailStatus;
    private TaskDetailMetricEnum statusMetric;
    private TaskDetailOrderEnum orderBy;

    private TaskDetailQuery() {
    }

    /**
     * 按任务 ID 构建查询。
     *
     * @param taskId 任务 ID
     * @return 查询参数
     */
    public static TaskDetailQuery of(String taskId) {
        TaskDetailQuery query = new TaskDetailQuery();
        query.taskId = taskId;
        return query;
    }

    /**
     * 设置分页。
     */
    public TaskDetailQuery page(int pageNum, int pageSize) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        return this;
    }

    /**
     * 按明细 ID 查询单条。
     */
    public TaskDetailQuery detailId(String detailId) {
        this.detailId = detailId;
        return this;
    }

    /**
     * 按明细类型筛选。
     */
    public TaskDetailQuery detailType(String detailType) {
        this.detailType = detailType;
        return this;
    }

    /**
     * 按明细状态筛选（success / fail）。
     * <p>与 {@link #statusMetric(TaskDetailMetricEnum)} 成对使用，仅设状态不设指标会在查询时断言失败。
     */
    public TaskDetailQuery detailStatus(String detailStatus) {
        this.detailStatus = detailStatus;
        return this;
    }

    /**
     * 状态筛选所依据的 Meta 指标（DIFF / FAIL）。
     * <p>设置 {@link #detailStatus(String)} 时必填，禁止静默默认。
     */
    public TaskDetailQuery statusMetric(TaskDetailMetricEnum statusMetric) {
        this.statusMetric = statusMetric;
        return this;
    }

    /**
     * 显式排序（与指标无关时使用）。
     */
    public TaskDetailQuery orderBy(TaskDetailOrderEnum orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getDetailId() {
        return detailId;
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

    public TaskDetailMetricEnum getStatusMetric() {
        return statusMetric;
    }

    public TaskDetailOrderEnum getOrderBy() {
        return orderBy;
    }
}

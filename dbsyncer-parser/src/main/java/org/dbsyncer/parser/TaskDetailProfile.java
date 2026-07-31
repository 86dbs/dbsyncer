/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.TaskDetailQuery;

import java.util.Map;

/**
 * 任务执行明细（dbsyncer_task_detail）查询：与 meta / table_group 关联装配展示行。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-20 15:00
 */
public interface TaskDetailProfile {

    /**
     * 查询校验/迁移结果列表（存储侧过滤、排序、分页）。
     *
     * @param query 查询参数（含 taskId、分页、类型/状态筛选、指标与排序）
     * @return 分页结果
     */
    Paging queryResults(TaskDetailQuery query);

    /**
     * 查询单条明细。
     *
     * @param query 查询参数（需含 taskId、detailId）
     * @return 展示行，不存在时 null
     */
    Map<String, Object> getDetail(TaskDetailQuery query);
}

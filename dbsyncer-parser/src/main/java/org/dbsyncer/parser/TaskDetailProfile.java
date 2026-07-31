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
     * 连表查询校验/迁移结果列表（存储侧过滤、排序、分页）。
     *
     * @param query 查询参数（含 taskId、分页、类型/状态筛选、指标与排序）
     * @return 分页结果
     */
    Paging queryJoinedResults(TaskDetailQuery query);

    /**
     * 单条明细连表查询。
     *
     * @param taskId   任务 ID
     * @param detailId 明细 ID
     * @return 展示行，不存在时 null
     */
    Map<String, Object> getJoinedDetail(String taskId, String detailId);
}

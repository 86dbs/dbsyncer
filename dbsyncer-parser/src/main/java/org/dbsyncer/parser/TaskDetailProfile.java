/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.Paging;

import java.util.Comparator;
import java.util.Map;
import java.util.function.Predicate;

/**
 * 任务执行明细（dbsyncer_task_detail）查询：与 meta / table_group 关联装配展示行。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-20 15:00
 */
public interface TaskDetailProfile {

    /**
     * 查询校验/迁移结果列表(连表装配后应用侧过滤/排序/分页)。
     *
     * @param taskId     任务 ID
     * @param filter     行过滤(可空)
     * @param comparator 排序(可空)
     * @param pageNum    页码
     * @param pageSize   页大小
     * @param detailType 明细 TYPE 过滤(可空)
     * @return 分页结果
     */
    Paging queryJoinedResults(String taskId, Predicate<Map<String, Object>> filter,
                              Comparator<Map<String, Object>> comparator,
                              int pageNum, int pageSize, String detailType);

    /**
     * 单条明细连表查询。
     *
     * @param taskId   任务 ID
     * @param detailId 明细 ID
     * @return 展示行，不存在时 null
     */
    Map<String, Object> getJoinedDetail(String taskId, String detailId);
}

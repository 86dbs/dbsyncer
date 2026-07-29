/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.model.MetaIncrement;

import java.util.List;
import java.util.Map;

/**
 * 任务执行结果表（dbsyncer_meta）查询与计数操作。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface MetaProfile {

    /**
     * 按 Meta 主键 id 查询
     */
    Meta getMeta(String metaId);

    /**
     * 全部 Meta（含明细级）。优先使用 {@link #getTaskMetaAll()}。
     */
    List<Meta> getMetaAll();

    /**
     * 仅任务级 Meta（IS_TASK_DETAIL=0）。
     */
    List<Meta> getTaskMetaAll();

    /**
     * 按关联 ID + 任务层级查询 Meta（任务级：taskId=任务ID；明细级：taskId=table_group.id）。
     */
    Meta getMetaByTaskId(String refId, TaskLevelEnum taskLevelEnum);

    /**
     * 批量按关联 ID 查询明细级 Meta（IS_TASK_DETAIL=1）。
     */
    Map<String, Meta> getDetailMetaMap(List<String> refIds);

    /**
     * Meta 计数原子增量(严格走库)：按 {@link MetaIncrement} 落库自增，可为负数。
     */
    void incrementMeta(MetaIncrement increment);

    void deleteMetaByTableGroupIds(List<String> tableGroupIds);
}

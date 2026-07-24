/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

/**
 * 任务运行结果生命周期操作（按任务 ID 清理/重置 Meta 与 TASK_DETAIL）。
 * <p>任务主表 CRUD 见企业侧 TaskService。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface TaskProfile {

    /**
     * 删除任务下明细级 Meta（按 table_group.id 关联，须在 clear 分表之前调用）。
     *
     * @param taskId 任务 ID
     */
    void removeDetailMetasByTaskId(String taskId);

    /**
     * 清空任务运行结果：按 table_group.id 删明细 Meta 并 clear TASK_DETAIL；表映射仍在时补回空明细 Meta。
     */
    void clearTaskRunResults(String taskId);

    /**
     * 重置任务级 Meta 计数与 SNAPSHOT（保留行，state=READY）。
     */
    void resetTaskMeta(String taskId);
}

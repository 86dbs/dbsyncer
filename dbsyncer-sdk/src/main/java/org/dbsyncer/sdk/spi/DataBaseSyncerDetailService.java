/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.DatabaseMigrationDetailResult;
import org.dbsyncer.sdk.model.DatabaseMigrationSyncTask;

/**
 * 整库迁移任务明细 SPI（终态结果落库，运行进度见 {@link DatabaseMigrationSyncTask} 快照）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 13:46
 */
public interface DataBaseSyncerDetailService {

    /**
     * 保存单表单阶段终态结果（按 taskId + type + tableIndex 先删后插）。
     *
     * @param task   迁移任务
     * @param detail 明细结果（含 type、tableIndex、库表名与计数）
     */
    void saveResult(DatabaseMigrationSyncTask task, DatabaseMigrationDetailResult detail);

    /**
     * 按任务 ID 查询迁移明细列表。
     *
     * @param taskId 任务 ID
     * @return 分页结果
     */
    Paging queryByTaskId(String taskId);

    /**
     * 删除任务下全部迁移明细（重跑前可选调用）。
     *
     * @param taskId 任务 ID
     */
    void deleteByTaskId(String taskId);
}

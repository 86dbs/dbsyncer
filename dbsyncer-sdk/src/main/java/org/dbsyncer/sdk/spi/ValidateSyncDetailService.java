/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.ValidateSyncDetailResult;
import org.dbsyncer.sdk.model.ValidateSyncTask;

import java.util.Map;

/**
 * 订正校验任务明细 SPI（落库 / 查询 / 清除，与 {@link TaskService} 任务生命周期解耦）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-04 18:00
 */
public interface ValidateSyncDetailService {

    /**
     * 保存单表校验终态结果（按 taskId + type + 源/目标表名先删后插）。
     *
     * @param task   校验任务
     * @param detail 明细结果
     */
    void saveResult(ValidateSyncTask task, ValidateSyncDetailResult detail);

    /**
     * 按任务 ID 查询校验明细列表（按更新时间倒序）。
     *
     * @param taskId 任务 ID
     * @return 分页结果
     */
    Paging result(String taskId);

    /**
     * 删除任务下全部校验明细（重跑前调用）。
     *
     * @param taskId 任务 ID
     */
    void clearDetail(String taskId);

    /**
     * 对单条明细中尚未成功订正的差异尝试手动订正，并更新明细汇总列。
     *
     * @param detailId 明细主键
     * @return 更新后的明细（含 diffTotal、fixedTotal、content 等）
     */
    Map<String, Object> manualRevise(String detailId);
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;

import java.util.Map;

/**
 * 整库迁移任务明细 SPI（终态结果落库，运行进度见任务快照）。
 * <p>表级生命周期见 {@code meta.STATE}；类型级状态见 {@code DATA.status}；{@code IS_SUCCESS} 仅成败。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 13:46
 */
public interface DatabaseSyncDetailService {

    /**
     * 查询迁移明细列表（按更新时间倒序）。
     *
     * @param params 请求参数
     * @return 分页结果
     */
    Paging result(Map<String, String> params);

    /**
     * 按任务表映射对齐明细行（每种开启的迁移类型一行）：缺则预建、重复则去重、孤儿则删除。
     * <p>默认状态为未运行（READY）。
     *
     * @param taskId 迁移任务ID
     */
    void syncTaskTableMetaDetails(String taskId);

    /**
     * 整轮重跑前：重置全部明细状态为未运行，并清空差异载荷（不删行）。
     *
     * @param taskId 迁移任务ID
     */
    void resetTaskDetailsForNewRound(String taskId);

    /**
     * 任务整轮完成后：收尾残留运行中状态（DATA.status + 表明细 Meta.STATE）。
     *
     * @param taskId 迁移任务ID
     */
    void markRunningDetailsDone(String taskId);
}

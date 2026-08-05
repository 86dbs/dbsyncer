/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;

import java.util.Map;

/**
 * 订正校验任务明细 SPI
 * <p>表级生命周期见 {@code meta.STATE}；类型级状态见 {@code DATA.status}；{@code IS_SUCCESS} 仅成败。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-04 18:00
 */
public interface ValidateSyncDetailService {

    /**
     * 查询校验明细列表（按更新时间倒序）。
     *
     * @param params 请求参数
     * @return 分页结果
     */
    Paging result(Map<String, String> params);

    /**
     * 对单条明细中尚未成功订正的差异尝试手动订正，并更新明细汇总列。
     *
     * @param taskId   校验任务ID(明细分表定位)
     * @param detailId 明细主键
     * @return 更新后的明细（含 diffTotal、fixedTotal、content 等）
     */
    Map<String, Object> manualRevise(String taskId, String detailId);

    /**
     * 按任务表映射对齐明细行（每种开启的校验类型一行）：缺则预建、重复则去重、孤儿则删除。
     * <p>默认状态为未运行（READY），不删行以外的差异内容结构由重置接口负责。
     *
     * @param taskId 校验任务ID
     */
    void syncTaskTableMetaDetails(String taskId);

    /**
     * 整轮重跑前：重置全部明细状态为未运行，并清空差异载荷（不删行）。
     *
     * @param taskId 校验任务ID
     */
    void resetTaskDetailsForNewRound(String taskId);

    /**
     * 任务整轮完成后：收尾残留运行中状态（DATA.status + 表明细 Meta.STATE）。
     *
     * @param taskId 校验任务ID
     */
    void markRunningDetailsDone(String taskId);
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;

import java.util.Map;

/**
 * 订正校验任务明细 SPI
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
     * @param detailId 明细主键
     * @return 更新后的明细（含 diffTotal、fixedTotal、content 等）
     */
    Map<String, Object> manualRevise(String detailId);
}

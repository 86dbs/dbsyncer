/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.DatabaseMigrationSyncTask;

import java.util.Map;

/**
 * 整库迁移任务明细 SPI（终态结果落库，运行进度见 {@link DatabaseMigrationSyncTask} 快照）。
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

}

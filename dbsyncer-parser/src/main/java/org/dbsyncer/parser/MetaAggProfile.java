/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.MetaAgg;

import java.util.List;

/**
 * 同步任务 Meta 日汇总读写。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface MetaAggProfile {

    /**
     * 按统计日查询。
     */
    MetaAgg getByStatDate(String statDate);

    /**
     * 最近若干天（按 STAT_DATE 升序，最多 {@code days} 条）。
     */
    List<MetaAgg> listRecent(int days);

    /**
     * 新增或按 STAT_DATE 更新当日汇总。
     */
    void upsert(MetaAgg daily);

    /**
     * 删除早于指定统计日的行（不含 cutoff）。
     */
    void removeBefore(String cutoffStatDate);
}

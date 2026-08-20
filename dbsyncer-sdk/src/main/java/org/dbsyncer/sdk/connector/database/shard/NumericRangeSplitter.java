/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.shard;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.WorkItemIds;
import org.dbsyncer.sdk.model.shard.ShardSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 数值主键闭区间切分。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public final class NumericRangeSplitter {

    private NumericRangeSplitter() {
    }

    /**
     * 按数值主键闭区间切分为若干 RANGE shard。
     *
     * @param tableGroupId 表映射 ID
     * @param pkName       主键名
     * @param minPk        最小主键（含）
     * @param maxPk        最大主键（含）
     * @param chunkSize    每段主键跨度上界
     * @return shard 列表
     */
    public static List<ShardSpec> split(String tableGroupId, String pkName, long minPk, long maxPk, long chunkSize) {
        if (StringUtil.isBlank(tableGroupId) || chunkSize <= 0 || maxPk < minPk) {
            return Collections.emptyList();
        }
        List<ShardSpec> items = new ArrayList<>();
        long from = minPk;
        while (from <= maxPk) {
            long to = from + chunkSize - 1;
            if (to > maxPk) {
                to = maxPk;
            }
            String itemId = WorkItemIds.rangeOf(tableGroupId, from, to);
            items.add(ShardSpec.range(itemId, pkName, String.valueOf(from), String.valueOf(to)));
            if (to == Long.MAX_VALUE) {
                break;
            }
            from = to + 1;
        }
        return items;
    }
}

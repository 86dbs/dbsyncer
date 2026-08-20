/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.shard;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.WorkItemIds;

import java.util.ArrayList;
import java.util.List;

/**
 * 通用切片计划工厂。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public final class ShardPlans {

    private ShardPlans() {
    }

    /**
     * 按 HASH_MOD 生成等份切片。
     *
     * @param tableGroupId 表映射 ID
     * @param pk           主键名
     * @param mod          份数
     * @return 计划
     */
    public static ShardPlan hashMod(String tableGroupId, String pk, int mod) {
        if (StringUtil.isBlank(tableGroupId) || mod <= 1) {
            return ShardPlan.wholeTable(tableGroupId);
        }
        List<ShardSpec> shards = new ArrayList<>(mod);
        for (int i = 0; i < mod; i++) {
            shards.add(ShardSpec.hashMod(WorkItemIds.hashOf(tableGroupId, mod, i), pk, mod, i));
        }
        return ShardPlan.of(shards);
    }

    /**
     * 按行偏移切片。
     *
     * @param tableGroupId 表映射 ID
     * @param totalRows    总行数
     * @param chunk        每段行数
     * @return 计划
     */
    public static ShardPlan offsetByRows(String tableGroupId, long totalRows, long chunk) {
        if (StringUtil.isBlank(tableGroupId) || totalRows <= 0 || chunk <= 0) {
            return ShardPlan.wholeTable(tableGroupId);
        }
        if (totalRows <= chunk) {
            return ShardPlan.wholeTable(tableGroupId);
        }
        List<ShardSpec> shards = new ArrayList<>();
        long from = 0;
        while (from < totalRows) {
            long to = Math.min(from + chunk - 1, totalRows - 1);
            shards.add(ShardSpec.offset(WorkItemIds.offsetOf(tableGroupId, from, to), from, to));
            from = to + 1;
        }
        return shards.size() <= 1 ? ShardPlan.wholeTable(tableGroupId) : ShardPlan.of(shards);
    }
}

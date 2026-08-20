/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.shard;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 表内切片计划。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public final class ShardPlan {

    private final List<ShardSpec> shards;

    public ShardPlan(List<ShardSpec> shards) {
        if (CollectionUtils.isEmpty(shards)) {
            this.shards = Collections.emptyList();
        } else {
            this.shards = Collections.unmodifiableList(new ArrayList<>(shards));
        }
    }

    public static ShardPlan wholeTable() {
        return new ShardPlan(Collections.emptyList());
    }

    public static ShardPlan wholeTable(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return wholeTable();
        }
        return new ShardPlan(Collections.singletonList(ShardSpec.whole(tableGroupId)));
    }

    public static ShardPlan of(List<ShardSpec> shards) {
        return new ShardPlan(shards);
    }

    public List<ShardSpec> getShards() {
        return shards;
    }

    /**
     * 是否实际切成多段（多于 1 个非整表 shard）。
     *
     * @return true 已切分
     */
    public boolean isSplit() {
        if (shards.size() <= 1) {
            return false;
        }
        for (ShardSpec shard : shards) {
            if (shard != null && !shard.isWhole()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 取出全部 itemId；空计划返回空列表。
     *
     * @return itemId 列表
     */
    public List<String> itemIds() {
        if (shards.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> ids = new ArrayList<>(shards.size());
        for (ShardSpec shard : shards) {
            if (shard != null && StringUtil.isNotBlank(shard.getItemId())) {
                ids.add(shard.getItemId());
            }
        }
        return ids;
    }

    public ShardSpec findByItemId(String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return null;
        }
        for (ShardSpec shard : shards) {
            if (shard != null && StringUtil.equals(itemId, shard.getItemId())) {
                return shard;
            }
        }
        return null;
    }
}

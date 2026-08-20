/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.shard;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.WorkItemAssignment;
import org.dbsyncer.sdk.model.WorkItemIds;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 调度/迁移侧调用连接器切片的薄封装。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public final class ConnectorShardSupport {

    private ConnectorShardSupport() {
    }

    /**
     * 本节点应执行的 itemId（单机为整表）。
     *
     * @param clusterService 集群服务
     * @param tableGroupId   表映射 ID
     * @return itemId 列表
     */
    public static List<String> resolveLocalItems(ClusterService clusterService, String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return Collections.emptyList();
        }
        if (clusterService == null || clusterService.isStandalone()) {
            return Collections.singletonList(tableGroupId);
        }
        List<String> items = new ArrayList<>();
        List<WorkItemAssignment> assignments = clusterService.listLocalAssignments();
        if (CollectionUtils.isEmpty(assignments)) {
            return items;
        }
        for (WorkItemAssignment assignment : assignments) {
            if (assignment != null && WorkItemIds.belongsToTable(assignment.getItemId(), tableGroupId)) {
                items.add(assignment.getItemId());
            }
        }
        return items;
    }

    /**
     * itemId 还原的 ShardSpec 常缺 pk，用源表主键补全。
     *
     * @param shard       切片；可为 null
     * @param sourceTable 源表
     * @return 带 pk 的切片；整表或无法补全时原样返回
     */
    public static ShardSpec enrichPk(ShardSpec shard, Table sourceTable) {
        if (shard == null || StringUtil.isNotBlank(shard.payload(ShardSpec.KEY_PK))) {
            return shard;
        }
        List<Field> pks = PrimaryKeyUtil.findPrimaryKeyFields(sourceTable == null ? null : sourceTable.getColumn());
        if (CollectionUtils.isEmpty(pks) || pks.size() != 1 || pks.get(0) == null) {
            return shard;
        }
        String pk = pks.get(0).getName();
        switch (shard.getCapability()) {
            case RANGE:
                return ShardSpec.range(shard.getItemId(), pk,
                        shard.payload(ShardSpec.KEY_FROM), shard.payload(ShardSpec.KEY_TO));
            case HASH_MOD:
                return ShardSpec.hashMod(shard.getItemId(), pk,
                        NumberUtil.toInt(shard.payload(ShardSpec.KEY_MOD)),
                        NumberUtil.toInt(shard.payload(ShardSpec.KEY_INDEX)));
            default:
                return shard;
        }
    }
}

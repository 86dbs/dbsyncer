/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.shard;

import org.dbsyncer.sdk.model.Table;

/**
 * 切片规划请求：调度传入提示，连接器决定能否切及如何切。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public final class ShardPlanRequest {

    /**
     * 数值 PK RANGE 单片主键跨度上限；不超过则整表，超过则切分且单片不超过该值。
     */
    public static final long MAX_NUMERIC_RANGE_CHUNK = 600_0000L;

    private final String tableGroupId;
    private final Table sourceTable;
    private final String schema;
    private final int pageSize;
    private final int onlineNodeCount;

    public ShardPlanRequest(String tableGroupId, Table sourceTable, String schema, int pageSize, int onlineNodeCount) {
        this.tableGroupId = tableGroupId;
        this.sourceTable = sourceTable;
        this.schema = schema;
        this.pageSize = pageSize;
        this.onlineNodeCount = onlineNodeCount;
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public String getSchema() {
        return schema;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getOnlineNodeCount() {
        return onlineNodeCount;
    }

    /**
     * 文件 OFFSET 等按行切分时的建议跨度：pageSize * nodeCount * 2。
     *
     * @return 行跨度
     */
    public long suggestedRangeChunk() {
        int nodes = Math.max(1, onlineNodeCount);
        int page = Math.max(1, pageSize);
        return (long) page * nodes * 2L;
    }

    /**
     * 数值 PK RANGE 建议分片数：默认 nodeCount * 10（至少 2）；再与单片上限共同决定实际片数。
     *
     * @return 分片数
     */
    public int suggestedShardCount() {
        int nodes = Math.max(1, onlineNodeCount);
        return Math.max(2, nodes * 10);
    }

    /**
     * 数值 PK 单片跨度上限（含）：≤ 此值整表，&gt; 此值才切且单片不超过该上限。
     *
     * @return 主键跨度上限
     */
    public long maxNumericRangeChunk() {
        return MAX_NUMERIC_RANGE_CHUNK;
    }

    /**
     * HASH 建议份数：默认 nodeCount * 4（至少 2）。
     *
     * @return 份数
     */
    public int suggestedHashMod() {
        int nodes = Math.max(1, onlineNodeCount);
        return Math.max(2, nodes * 10);
    }
}

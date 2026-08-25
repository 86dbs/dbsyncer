/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.workitem;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.WorkBoundType;

/**
 * 单个工作项边界：调度认 itemId；执行侧按类型消费边界字段。
 * <p>由 Leader 规划后下发，Worker 不再二次划界。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-25
 */
public final class WorkBound {

    private final String itemId;
    private final WorkBoundType type;
    private final String pk;
    private final String from;
    private final long rowBudget;
    private final long offsetStart;
    private final long offsetEnd;
    private final String partitionId;

    private WorkBound(String itemId, WorkBoundType type, String pk, String from, long rowBudget,
                     long offsetStart, long offsetEnd, String partitionId) {
        this.itemId = itemId;
        this.type = type == null ? WorkBoundType.NONE : type;
        this.pk = pk == null ? StringUtil.EMPTY : pk;
        this.from = from == null ? StringUtil.EMPTY : from;
        this.rowBudget = rowBudget;
        this.offsetStart = offsetStart;
        this.offsetEnd = offsetEnd;
        this.partitionId = partitionId == null ? StringUtil.EMPTY : partitionId;
    }

    /**
     * 整表工作项。
     *
     * @param tableGroupId 表映射 ID
     * @return 边界
     */
    public static WorkBound whole(String tableGroupId) {
        return new WorkBound(tableGroupId, WorkBoundType.NONE, null, null, 0L, 0L, 0L, null);
    }

    /**
     * 游标分批：起始游标（空=表头；非空表示字典序 {@code pk > from}）+ 行预算。
     *
     * @param itemId    工作项 ID
     * @param pk        定位键名，可空；复合主键逗号分隔
     * @param from      排他起始游标；空表示表头
     * @param rowBudget 本项最多读取行数
     * @return 边界
     */
    public static WorkBound cursorBatch(String itemId, String pk, String from, long rowBudget) {
        return new WorkBound(itemId, WorkBoundType.CURSOR_BATCH, pk, from, rowBudget, 0L, 0L, null);
    }

    /**
     * 偏移区间工作项。
     *
     * @param itemId 工作项 ID
     * @param start  起始（含）
     * @param end    结束（含）
     * @return 边界
     */
    public static WorkBound offset(String itemId, long start, long end) {
        return new WorkBound(itemId, WorkBoundType.OFFSET, null, null, 0L, start, end, null);
    }

    /**
     * 分区工作项。
     *
     * @param itemId      工作项 ID
     * @param partitionId 分区标识
     * @return 边界
     */
    public static WorkBound partition(String itemId, String partitionId) {
        return new WorkBound(itemId, WorkBoundType.PARTITION, null, null, 0L, 0L, 0L, partitionId);
    }

    public String getItemId() {
        return itemId;
    }

    public WorkBoundType getType() {
        return type;
    }

    /**
     * 定位键名（CURSOR_BATCH）；复合主键逗号分隔。
     *
     * @return 主键名；可能为空
     */
    public String getPk() {
        return pk;
    }

    /**
     * 排他起始游标文本（CURSOR_BATCH）。
     *
     * @return 游标；空表示表头
     */
    public String getFrom() {
        return from;
    }

    /**
     * 行预算（CURSOR_BATCH）。
     *
     * @return 行数；&lt;=0 表示未限制
     */
    public long getRowBudget() {
        return rowBudget;
    }

    public long getOffsetStart() {
        return offsetStart;
    }

    public long getOffsetEnd() {
        return offsetEnd;
    }

    public String getPartitionId() {
        return partitionId;
    }

    /**
     * 是否整表（无边界）。
     *
     * @return true 整表
     */
    public boolean isWhole() {
        return type == WorkBoundType.NONE;
    }
}

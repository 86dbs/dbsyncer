/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;

/**
 * WorkItem ID 约定：整表为 {@code tableGroupId}；数值 PK range 为 {@code tableGroupId#from#to}。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-19
 */
public final class WorkItemIds {

    private static final char SEP = '#';

    private WorkItemIds() {
    }

    /**
     * 组装 range itemId。
     *
     * @param tableGroupId 表映射 ID
     * @param fromInclusive 下界（含）
     * @param toInclusive   上界（含）
     * @return itemId
     */
    public static String rangeOf(String tableGroupId, long fromInclusive, long toInclusive) {
        return tableGroupId + SEP + fromInclusive + SEP + toInclusive;
    }

    /**
     * 是否 range item。
     *
     * @param itemId 工作项 ID
     * @return true range
     */
    public static boolean isRange(String itemId) {
        return parse(itemId) != null;
    }

    /**
     * 解析所属表映射 ID（整表或 range）。
     *
     * @param itemId 工作项 ID
     * @return tableGroupId；非法为空
     */
    public static String tableGroupIdOf(String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return StringUtil.EMPTY;
        }
        Range range = parse(itemId);
        return range == null ? itemId : range.getTableGroupId();
    }

    /**
     * item 是否属于指定表。
     *
     * @param itemId       工作项
     * @param tableGroupId 表映射 ID
     * @return true 属于
     */
    public static boolean belongsToTable(String itemId, String tableGroupId) {
        if (StringUtil.isBlank(itemId) || StringUtil.isBlank(tableGroupId)) {
            return false;
        }
        return StringUtil.equals(tableGroupId, tableGroupIdOf(itemId));
    }

    /**
     * 解析 range；非 range 返回 null。
     *
     * @param itemId 工作项 ID
     * @return Range 或 null
     */
    public static Range parse(String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return null;
        }
        int last = itemId.lastIndexOf(SEP);
        if (last <= 0) {
            return null;
        }
        int mid = itemId.lastIndexOf(SEP, last - 1);
        if (mid <= 0) {
            return null;
        }
        String tableGroupId = itemId.substring(0, mid);
        String fromText = itemId.substring(mid + 1, last);
        String toText = itemId.substring(last + 1);
        if (StringUtil.isBlank(tableGroupId) || !NumberUtil.isCreatable(fromText) || !NumberUtil.isCreatable(toText)) {
            return null;
        }
        long from = NumberUtil.toLong(fromText);
        long to = NumberUtil.toLong(toText);
        if (to < from) {
            return null;
        }
        return new Range(tableGroupId, from, to);
    }

    /**
     * 数值主键闭区间。
     */
    public static final class Range {
        private final String tableGroupId;
        private final long fromInclusive;
        private final long toInclusive;

        public Range(String tableGroupId, long fromInclusive, long toInclusive) {
            this.tableGroupId = tableGroupId;
            this.fromInclusive = fromInclusive;
            this.toInclusive = toInclusive;
        }

        public String getTableGroupId() {
            return tableGroupId;
        }

        public long getFromInclusive() {
            return fromInclusive;
        }

        public long getToInclusive() {
            return toInclusive;
        }

        public String toItemId() {
            return rangeOf(tableGroupId, fromInclusive, toInclusive);
        }
    }
}

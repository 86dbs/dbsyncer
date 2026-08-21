/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.ShardSupportEnum;
import org.dbsyncer.sdk.model.shard.ShardSpec;

import java.util.Collections;

/**
 * WorkItem ID 约定：
 * <ul>
 *   <li>整表：{@code tableGroupId}</li>
 *   <li>数值 RANGE：{@code tableGroupId#from#to}</li>
 *   <li>HASH：{@code tableGroupId#h#mod#index}</li>
 *   <li>OFFSET：{@code tableGroupId#o#start#end}</li>
 *   <li>PARTITION（解析预留）：{@code tableGroupId#p#id}</li>
 * </ul>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-19
 */
public final class WorkItemIds {

    private static final char SEP = '#';
    private static final String MARK_HASH = "#h#";
    private static final String MARK_OFFSET = "#o#";
    private static final String MARK_PARTITION = "#p#";

    private WorkItemIds() {
    }

    /**
     * 组装数值 range itemId。
     *
     * @param tableGroupId  表映射 ID
     * @param fromInclusive 下界（含）
     * @param toInclusive   上界（含）
     * @return itemId
     */
    public static String rangeOf(String tableGroupId, long fromInclusive, long toInclusive) {
        return tableGroupId + SEP + fromInclusive + SEP + toInclusive;
    }

    /**
     * HASH 取模 itemId。
     *
     * @param tableGroupId 表映射 ID
     * @param mod          模数
     * @param index        桶下标
     * @return itemId
     */
    public static String hashOf(String tableGroupId, int mod, int index) {
        return tableGroupId + MARK_HASH + mod + SEP + index;
    }

    /**
     * 偏移 itemId。
     *
     * @param tableGroupId 表映射 ID
     * @param start        起始（含）
     * @param end          结束（含）
     * @return itemId
     */
    public static String offsetOf(String tableGroupId, long start, long end) {
        return tableGroupId + MARK_OFFSET + start + SEP + end;
    }

    /**
     * 是否为表内切片 item（非整表）。
     *
     * @param itemId 工作项 ID
     * @return true 切片
     */
    public static boolean isShard(String itemId) {
        return parseShard(itemId) != null;
    }

    /**
     * 是否 range item。
     *
     * @param itemId 工作项 ID
     * @return true range
     */
    public static boolean isRange(String itemId) {
        ShardRef ref = parseShard(itemId);
        return ref != null && ref.getCapability() == ShardSupportEnum.RANGE;
    }

    /**
     * 解析所属表映射 ID（整表或切片）。
     *
     * @param itemId 工作项 ID
     * @return tableGroupId；非法为空
     */
    public static String tableGroupIdOf(String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return StringUtil.EMPTY;
        }
        ShardRef ref = parseShard(itemId);
        return ref == null ? itemId : ref.getTableGroupId();
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
     * 是否为任务级整项（增量 Mapping 派工：itemId 与 taskId 相同，不拆表）。
     *
     * @param taskId 任务 ID
     * @param itemId 工作项 ID
     * @return true 任务级
     */
    public static boolean isTaskLevelItem(String taskId, String itemId) {
        return StringUtil.isNotBlank(taskId) && StringUtil.equals(taskId, itemId);
    }

    /**
     * 解析数值闭区间 range；非 RANGE 返回 null。
     *
     * @param itemId 工作项 ID
     * @return Range 或 null
     */
    public static Range parse(String itemId) {
        ShardRef ref = parseShard(itemId);
        if (ref == null || ref.getCapability() != ShardSupportEnum.RANGE) {
            return null;
        }
        if (!NumberUtil.isCreatable(ref.getPart1()) || !NumberUtil.isCreatable(ref.getPart2())) {
            return null;
        }
        long from = NumberUtil.toLong(ref.getPart1());
        long to = NumberUtil.toLong(ref.getPart2());
        if (to < from) {
            return null;
        }
        return new Range(ref.getTableGroupId(), from, to);
    }

    /**
     * 从 itemId 还原可执行的 ShardSpec（无 pk 时由连接器再用表元数据补）。
     *
     * @param itemId 工作项
     * @return ShardSpec；整表或无法解析为 null
     */
    public static ShardSpec toShardSpec(String itemId) {
        ShardRef ref = parseShard(itemId);
        if (ref == null) {
            return null;
        }
        switch (ref.getCapability()) {
            case RANGE:
                return ShardSpec.range(itemId, null, ref.getPart1(), ref.getPart2());
            case HASH_MOD:
                if (!NumberUtil.isCreatable(ref.getPart1()) || !NumberUtil.isCreatable(ref.getPart2())) {
                    return null;
                }
                return ShardSpec.hashMod(itemId, null,
                        NumberUtil.toInt(ref.getPart1()), NumberUtil.toInt(ref.getPart2()));
            case OFFSET:
                if (!NumberUtil.isCreatable(ref.getPart1()) || !NumberUtil.isCreatable(ref.getPart2())) {
                    return null;
                }
                return ShardSpec.offset(itemId, NumberUtil.toLong(ref.getPart1()), NumberUtil.toLong(ref.getPart2()));
            case PARTITION:
                return new ShardSpec(itemId, ShardSupportEnum.PARTITION,
                        Collections.singletonMap(ShardSpec.KEY_PARTITION_ID, ref.getPart1()));
            default:
                return null;
        }
    }

    /**
     * 解析切片引用。
     *
     * @param itemId 工作项
     * @return 切片；整表或非法为 null
     */
    public static ShardRef parseShard(String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return null;
        }
        ShardRef typed = parseTyped(itemId, MARK_HASH, ShardSupportEnum.HASH_MOD, true);
        if (typed != null) {
            return typed;
        }
        typed = parseTyped(itemId, MARK_OFFSET, ShardSupportEnum.OFFSET, true);
        if (typed != null) {
            return typed;
        }
        typed = parseTyped(itemId, MARK_PARTITION, ShardSupportEnum.PARTITION, false);
        if (typed != null) {
            return typed;
        }
        return parseNumericRange(itemId);
    }

    private static ShardRef parseTyped(String itemId, String mark, ShardSupportEnum capability, boolean twoParts) {
        int idx = itemId.lastIndexOf(mark);
        if (idx <= 0) {
            return null;
        }
        String tableGroupId = itemId.substring(0, idx);
        String rest = itemId.substring(idx + mark.length());
        if (StringUtil.isBlank(tableGroupId) || StringUtil.isBlank(rest)) {
            return null;
        }
        if (!twoParts) {
            return new ShardRef(tableGroupId, capability, rest, StringUtil.EMPTY);
        }
        int split = rest.lastIndexOf(SEP);
        if (split <= 0 || split >= rest.length() - 1) {
            return null;
        }
        return new ShardRef(tableGroupId, capability, rest.substring(0, split), rest.substring(split + 1));
    }

    private static ShardRef parseNumericRange(String itemId) {
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
        return new ShardRef(tableGroupId, ShardSupportEnum.RANGE, fromText, toText);
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

    /**
     * 切片解析中间结果。
     */
    public static final class ShardRef {
        private final String tableGroupId;
        private final ShardSupportEnum capability;
        private final String part1;
        private final String part2;

        public ShardRef(String tableGroupId, ShardSupportEnum capability, String part1, String part2) {
            this.tableGroupId = tableGroupId;
            this.capability = capability;
            this.part1 = part1;
            this.part2 = part2;
        }

        public String getTableGroupId() {
            return tableGroupId;
        }

        public ShardSupportEnum getCapability() {
            return capability;
        }

        public String getPart1() {
            return part1;
        }

        public String getPart2() {
            return part2;
        }
    }
}

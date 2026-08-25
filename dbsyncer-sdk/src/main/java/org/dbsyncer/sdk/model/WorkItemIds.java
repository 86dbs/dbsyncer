/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.WorkBoundType;
import org.dbsyncer.sdk.model.workitem.WorkBound;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * 工作项 ID 约定：
 * <ul>
 *   <li>整表：{@code tableGroupId}</li>
 *   <li>游标分批：{@code tableGroupId#cb#startCursor#budget}（startCursor 经 URL 编码，可空；
 *       复合主键为逗号拼接后整体编码，与进度游标格式一致；budget 为正整数行预算；start 表示排他游标）</li>
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
    private static final String MARK_OFFSET = "#o#";
    private static final String MARK_PARTITION = "#p#";
    private static final String MARK_CURSOR_BATCH = "#cb#";
    private static final String UTF_8 = StandardCharsets.UTF_8.name();

    private WorkItemIds() {
    }

    /**
     * 游标分批 itemId：排他起始游标 + 行预算。
     *
     * @param tableGroupId 表映射 ID
     * @param startCursor  排他起始游标文本；空表示表头
     * @param rowBudget    本项最多读取行数（须 &gt; 0）
     * @return itemId
     */
    public static String cursorBatchOf(String tableGroupId, String startCursor, long rowBudget) {
        long budget = Math.max(1L, rowBudget);
        return tableGroupId + MARK_CURSOR_BATCH + encode(startCursor) + SEP + budget;
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
     * 是否为带边界的拆分工作项（非整表）。
     *
     * @param itemId 工作项 ID
     * @return true 已拆分
     */
    public static boolean isSplitItem(String itemId) {
        return parseBound(itemId) != null;
    }

    /**
     * 是否游标分批工作项。
     *
     * @param itemId 工作项 ID
     * @return true 游标分批
     */
    public static boolean isCursorBatch(String itemId) {
        BoundRef ref = parseBound(itemId);
        return ref != null && ref.getType() == WorkBoundType.CURSOR_BATCH;
    }

    /**
     * 解析所属表映射 ID（整表或拆分项）。
     *
     * @param itemId 工作项 ID
     * @return tableGroupId；非法为空
     */
    public static String tableGroupIdOf(String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return StringUtil.EMPTY;
        }
        BoundRef ref = parseBound(itemId);
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
     * 从 itemId 还原可执行边界（无 pk 时由执行侧再用表元数据补）。
     *
     * @param itemId 工作项
     * @return 边界；整表或无法解析为 null
     */
    public static WorkBound toWorkBound(String itemId) {
        BoundRef ref = parseBound(itemId);
        if (ref == null) {
            return null;
        }
        switch (ref.getType()) {
            case CURSOR_BATCH:
                if (!NumberUtil.isCreatable(ref.getPart2()) || !isPlainBudgetToken(ref.getPart2())) {
                    return null;
                }
                return WorkBound.cursorBatch(itemId, null, ref.getPart1(), NumberUtil.toLong(ref.getPart2()));
            case OFFSET:
                if (!NumberUtil.isCreatable(ref.getPart1()) || !NumberUtil.isCreatable(ref.getPart2())) {
                    return null;
                }
                return WorkBound.offset(itemId, NumberUtil.toLong(ref.getPart1()), NumberUtil.toLong(ref.getPart2()));
            case PARTITION:
                return WorkBound.partition(itemId, ref.getPart1());
            default:
                return null;
        }
    }

    /**
     * 解析工作项边界引用。
     *
     * @param itemId 工作项
     * @return 引用；整表或非法为 null
     */
    public static BoundRef parseBound(String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return null;
        }
        BoundRef typed = parseTyped(itemId, MARK_CURSOR_BATCH, WorkBoundType.CURSOR_BATCH, true, true);
        if (typed != null) {
            return typed;
        }
        typed = parseTyped(itemId, MARK_OFFSET, WorkBoundType.OFFSET, true, false);
        if (typed != null) {
            return typed;
        }
        return parseTyped(itemId, MARK_PARTITION, WorkBoundType.PARTITION, false, false);
    }

    private static boolean isPlainBudgetToken(String token) {
        if (StringUtil.isBlank(token)) {
            return false;
        }
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    private static BoundRef parseTyped(String itemId, String mark, WorkBoundType type,
                                       boolean twoParts, boolean decodeParts) {
        int idx = itemId.lastIndexOf(mark);
        if (idx <= 0) {
            return null;
        }
        String tableGroupId = itemId.substring(0, idx);
        String rest = itemId.substring(idx + mark.length());
        if (StringUtil.isBlank(tableGroupId)) {
            return null;
        }
        if (!twoParts) {
            if (StringUtil.isBlank(rest)) {
                return null;
            }
            return new BoundRef(tableGroupId, type, rest, StringUtil.EMPTY);
        }
        int split = rest.lastIndexOf(SEP);
        if (split < 0 || split >= rest.length() - 1) {
            return null;
        }
        String part1 = rest.substring(0, split);
        String part2 = rest.substring(split + 1);
        if (decodeParts) {
            part1 = decode(part1);
            if (!(type == WorkBoundType.CURSOR_BATCH && isPlainBudgetToken(part2))) {
                part2 = decode(part2);
            }
        }
        if (StringUtil.isBlank(part2)) {
            return null;
        }
        return new BoundRef(tableGroupId, type, part1, part2);
    }

    private static String encode(String raw) {
        if (raw == null || raw.isEmpty()) {
            return StringUtil.EMPTY;
        }
        try {
            return URLEncoder.encode(raw, UTF_8);
        } catch (UnsupportedEncodingException e) {
            return raw;
        }
    }

    private static String decode(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return StringUtil.EMPTY;
        }
        try {
            return URLDecoder.decode(encoded, UTF_8);
        } catch (UnsupportedEncodingException e) {
            return encoded;
        }
    }

    /**
     * 工作项边界解析中间结果。
     */
    public static final class BoundRef {
        private final String tableGroupId;
        private final WorkBoundType type;
        private final String part1;
        private final String part2;

        public BoundRef(String tableGroupId, WorkBoundType type, String part1, String part2) {
            this.tableGroupId = tableGroupId;
            this.type = type;
            this.part1 = part1;
            this.part2 = part2;
        }

        public String getTableGroupId() {
            return tableGroupId;
        }

        public WorkBoundType getType() {
            return type;
        }

        public String getPart1() {
            return part1;
        }

        public String getPart2() {
            return part2;
        }
    }
}

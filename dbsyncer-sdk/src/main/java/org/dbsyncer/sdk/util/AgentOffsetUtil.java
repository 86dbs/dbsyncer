/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.AgentOffsetConstant;

import java.util.HashMap;
import java.util.Map;

/**
 * Agent 采集位点读写与比较。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-17
 */
public final class AgentOffsetUtil {

    private AgentOffsetUtil() {
    }

    /**
     * 从 meta.snapshot 取出可上报的恢复位点；缺 sequence 时返回 null。
     */
    public static Map<String, Object> readCheckpoint(Map<String, String> snapshot) {
        if (CollectionUtils.isEmpty(snapshot)) {
            return null;
        }
        String sequence = snapshot.get(AgentOffsetConstant.REDO_SEQUENCE);
        if (StringUtil.isBlank(sequence)) {
            return null;
        }
        Map<String, Object> checkpoint = new HashMap<>(8);
        checkpoint.put("sequence", sequence.trim());
        checkpoint.put("block", defaultNumber(snapshot.get(AgentOffsetConstant.REDO_BLOCK), "1"));
        checkpoint.put("offset", defaultNumber(snapshot.get(AgentOffsetConstant.REDO_OFFSET), "0"));
        String scn = snapshot.get(AgentOffsetConstant.REDO_SCN);
        if (StringUtil.isNotBlank(scn)) {
            checkpoint.put("scn", scn.trim());
        }
        String path = snapshot.get(AgentOffsetConstant.REDO_PATH);
        if (StringUtil.isNotBlank(path)) {
            checkpoint.put("path", path.trim());
        }
        return checkpoint;
    }

    /**
     * 从事件字段组装 ChangedOffset.position。
     */
    public static Map<String, Object> toPosition(Object scn, Object sequence, Object block, Object offset, Object path) {
        Map<String, Object> position = new HashMap<>(8);
        if (scn != null) {
            position.put("scn", scn);
        }
        if (sequence != null) {
            position.put("sequence", sequence);
        }
        if (block != null) {
            position.put("block", block);
        }
        if (offset != null) {
            position.put("offset", offset);
        }
        if (path != null && StringUtil.isNotBlank(String.valueOf(path))) {
            position.put("path", path);
        }
        return position;
    }

    /**
     * 取更旧的位点；任一为空则返回另一个。
     */
    public static Map<String, Object> min(Map<String, Object> left, Map<String, Object> right) {
        if (left == null || left.isEmpty()) {
            return right;
        }
        if (right == null || right.isEmpty()) {
            return left;
        }
        return compare(left, right) <= 0 ? left : right;
    }

    /**
     * 比较两个位点：sequence → block → offset。负值表示 left 更旧。
     */
    public static int compare(Map<String, Object> left, Map<String, Object> right) {
        long ls = longValue(left, "sequence");
        long rs = longValue(right, "sequence");
        if (ls != rs) {
            return Long.compare(ls, rs);
        }
        long lb = longValue(left, "block");
        long rb = longValue(right, "block");
        if (lb != rb) {
            return Long.compare(lb, rb);
        }
        return Integer.compare(intValue(left, "offset"), intValue(right, "offset"));
    }

    public static long longValue(Map<String, Object> map, String key) {
        if (map == null || !map.containsKey(key) || map.get(key) == null) {
            return 0L;
        }
        return NumberUtil.toLong(String.valueOf(map.get(key)), 0L);
    }

    public static int intValue(Map<String, Object> map, String key) {
        if (map == null || !map.containsKey(key) || map.get(key) == null) {
            return 0;
        }
        return NumberUtil.toInt(String.valueOf(map.get(key)), 0);
    }

    private static String defaultNumber(String value, String defaultValue) {
        return StringUtil.isBlank(value) ? defaultValue : value.trim();
    }
}

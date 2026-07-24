/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * 任务执行明细分表(dbsyncer_task_detail)工具。
 * <p>校验/迁移结果展示字段来自连表：table_group(关联列) + meta(指标) + task_detail(载荷)。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-17 18:00
 */
public abstract class TaskDetailUtil {

    /**
     * MySQL BLOB 上限（与 dbsyncer_task_detail.DATA 一致），写入前必须截取以免 Data truncation。
     */
    public static final int MAX_DATA_BYTES = 65535;

    private TaskDetailUtil() {
    }

    /**
     * 结构化内容序列化为 DATA blob 字节
     * <p>超过 {@link #MAX_DATA_BYTES} 时优先截断 {@code content} 字段，尽量保持 JSON 可解析。
     *
     * @param content 结构化字段
     * @return blob 字节，内容为空时返回 null
     */
    public static byte[] serializeContent(Map<String, Object> content) {
        if (content == null || content.isEmpty()) {
            return null;
        }
        byte[] bytes = toUtf8Bytes(content);
        if (bytes.length <= MAX_DATA_BYTES) {
            return bytes;
        }
        // 优先截断载荷字段，保证入库且尽量可反序列化
        Map<String, Object> copy = new HashMap<>(content);
        Object raw = copy.get(ConfigConstant.TASK_CONTENT);
        if (raw instanceof String && StringUtil.isNotBlank((String) raw)) {
            copy.put(ConfigConstant.TASK_CONTENT, StringUtil.EMPTY);
            int overhead = toUtf8Bytes(copy).length;
            int budget = Math.max(0, MAX_DATA_BYTES - overhead);
            copy.put(ConfigConstant.TASK_CONTENT, truncateUtf8((String) raw, budget));
            bytes = toUtf8Bytes(copy);
            if (bytes.length <= MAX_DATA_BYTES) {
                return bytes;
            }
        }
        // 兜底：按字节硬截断（可能破坏 JSON，反序列化失败时返回空 Map）
        return truncateBytes(bytes, MAX_DATA_BYTES);
    }

    private static byte[] toUtf8Bytes(Map<String, Object> content) {
        return JsonUtil.objToJsonSafe(content).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 按 UTF-8 字节上限截断字符串，保证落在完整字符边界上。
     */
    private static String truncateUtf8(String value, int maxBytes) {
        if (value == null || maxBytes <= 0) {
            return StringUtil.EMPTY;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) {
            return value;
        }
        int end = maxBytes;
        // 若切断点落在多字节字符中间，回退到该字符起始之前
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) {
            end--;
        }
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }

    private static byte[] truncateBytes(byte[] bytes, int maxBytes) {
        if (bytes == null || bytes.length <= maxBytes) {
            return bytes;
        }
        int end = maxBytes;
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) {
            end--;
        }
        byte[] truncated = new byte[end];
        System.arraycopy(bytes, 0, truncated, 0, end);
        return truncated;
    }

    /**
     * 将 DATA blob 反序列化为结构化内容 Map
     *
     * @param dataBlob 明细 DATA 列值(byte[] 或 String)
     * @return 结构化字段，异常或为空时返回空 Map
     */
    public static Map<String, Object> deserializeContent(Object dataBlob) {
        String json = null;
        if (dataBlob instanceof byte[]) {
            byte[] b = (byte[]) dataBlob;
            if (b.length > 0) {
                json = new String(b, StandardCharsets.UTF_8);
            }
        } else if (dataBlob instanceof String) {
            json = (String) dataBlob;
        }
        if (json == null || json.isEmpty()) {
            return new HashMap<>();
        }
        try {
            Map<String, Object> map = JsonUtil.parseMap(json);
            return map == null ? new HashMap<>() : map;
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    /**
     * 读明细行时把 DATA blob 的结构化字段合并回行 Map，并补齐前端所需的 status(取自 isSuccess)。
     *
     * @param row 明细行(键为 labelName)
     * @return 合并后的行 Map
     */
    public static Map<String, Object> mergeDetailRow(Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        Map<String, Object> content = deserializeContent(row.get(ConfigConstant.BINLOG_DATA));
        if (!content.isEmpty()) {
            content.forEach(row::putIfAbsent);
        }
        // isSuccess 即执行状态(完成=1/运行中=0)，前端读 status
        if (!row.containsKey(ConfigConstant.TASK_STATUS) && row.containsKey(ConfigConstant.DETAIL_IS_SUCCESS)) {
            row.put(ConfigConstant.TASK_STATUS, row.get(ConfigConstant.DETAIL_IS_SUCCESS));
        }
        // TARGET_TABLE(targetTable) 映射为前端 VO 所需 targetTableName
        if (!row.containsKey(ConfigConstant.DATA_TARGET_TABLE_NAME) && row.containsKey(ConfigConstant.DETAIL_TARGET_TABLE)) {
            row.put(ConfigConstant.DATA_TARGET_TABLE_NAME, row.get(ConfigConstant.DETAIL_TARGET_TABLE));
        }
        return row;
    }

    /**
     * 装配校验/迁移结果行：table_group 关联列 + meta 指标 + detail 载荷。
     *
     * @param detailRow     task_detail 行
     * @param tableGroupRow table_group 展示字段(可空)
     * @param metaRow       meta 展示字段(可空)
     * @return 合并后的展示行
     */
    public static Map<String, Object> assembleJoinedRow(Map<String, Object> detailRow,
                                                        Map<String, Object> tableGroupRow,
                                                        Map<String, Object> metaRow) {
        Map<String, Object> row = detailRow == null ? new HashMap<>() : new HashMap<>(detailRow);
        mergeDetailRow(row);
        if (tableGroupRow != null) {
            putIfPresent(row, ConfigConstant.TASK_SOURCE_TABLE_NAME, tableGroupRow.get(ConfigConstant.TABLE_GROUP_SOURCE_TABLE));
            putIfPresent(row, ConfigConstant.DATA_TARGET_TABLE_NAME, tableGroupRow.get(ConfigConstant.TABLE_GROUP_TARGET_TABLE));
            putIfPresent(row, ConfigConstant.DETAIL_TARGET_TABLE, tableGroupRow.get(ConfigConstant.TABLE_GROUP_TARGET_TABLE));
            putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_DATABASE, tableGroupRow.get(ConfigConstant.TABLE_GROUP_SOURCE_DATABASE));
            putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_SCHEMA, tableGroupRow.get(ConfigConstant.TABLE_GROUP_SOURCE_SCHEMA));
            putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_DATABASE, tableGroupRow.get(ConfigConstant.TABLE_GROUP_TARGET_DATABASE));
            putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_SCHEMA, tableGroupRow.get(ConfigConstant.TABLE_GROUP_TARGET_SCHEMA));
            putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_TABLE, tableGroupRow.get(ConfigConstant.TABLE_GROUP_SOURCE_TABLE));
            putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_TABLE, tableGroupRow.get(ConfigConstant.TABLE_GROUP_TARGET_TABLE));
            putIfPresent(row, ConfigConstant.TASK_SOURCE_TOTAL, tableGroupRow.get(ConfigConstant.TABLE_GROUP_SOURCE_TOTAL));
            putIfPresent(row, ConfigConstant.TASK_TARGET_TOTAL, tableGroupRow.get(ConfigConstant.TABLE_GROUP_TARGET_TOTAL));
            putIfPresent(row, ConfigConstant.TABLE_GROUP_SORT_INDEX, tableGroupRow.get(ConfigConstant.TABLE_GROUP_SORT_INDEX));
        }
        if (metaRow != null) {
            putIfPresent(row, ConfigConstant.TASK_DIFF_TOTAL, metaRow.get(ConfigConstant.META_DIFF));
            putIfPresent(row, ConfigConstant.TASK_FIXED_TOTAL, metaRow.get(ConfigConstant.META_FIXED));
            putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_SUCCESS_TOTAL, metaRow.get(ConfigConstant.META_SUCCESS));
            putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_FAIL_TOTAL, metaRow.get(ConfigConstant.META_FAIL));
            putIfPresent(row, ConfigConstant.META_TOTAL, metaRow.get(ConfigConstant.META_TOTAL));
            putIfPresent(row, ConfigConstant.META_STATE, metaRow.get(ConfigConstant.META_STATE));
            // 状态优先用 meta.state；前端兼容 status
            if (metaRow.get(ConfigConstant.META_STATE) != null) {
                row.put(ConfigConstant.TASK_STATUS, metaRow.get(ConfigConstant.META_STATE));
            }
        }
        return row;
    }

    private static void putIfPresent(Map<String, Object> row, String key, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof String && StringUtil.isBlank((String) value)) {
            return;
        }
        row.put(key, value);
    }

    /**
     * 将 Meta 模型转为展示字段 Map
     */
    public static Map<String, Object> toMetaDisplayMap(Object total, Object success, Object fail,
                                                       Object diff, Object fixed, Object state) {
        Map<String, Object> map = new HashMap<>();
        map.put(ConfigConstant.META_TOTAL, total);
        map.put(ConfigConstant.META_SUCCESS, success);
        map.put(ConfigConstant.META_FAIL, fail);
        map.put(ConfigConstant.META_DIFF, diff);
        map.put(ConfigConstant.META_FIXED, fixed);
        map.put(ConfigConstant.META_STATE, state);
        return map;
    }

    /**
     * 明细分表查询后的统一后处理：合并 DATA blob → 应用过滤 → 排序 → 分页。
     *
     * @param rows       原始行集合(每行为 Map)
     * @param filter     过滤条件(可空)
     * @param comparator 排序器(可空)
     * @param pageNum    页码(从 1 开始)
     * @param pageSize   每页条数
     * @return 分页结果
     */
    public static Paging pageDetails(Collection<?> rows, Predicate<Map<String, Object>> filter,
                                     Comparator<Map<String, Object>> comparator, int pageNum, int pageSize) {
        List<Map<String, Object>> merged = mergeAll(rows, filter);
        if (comparator != null) {
            merged.sort(comparator);
        }
        int safePageNum = Math.max(1, pageNum);
        int safePageSize = Math.max(1, pageSize);
        Paging paging = new Paging(safePageNum, safePageSize);
        paging.setTotal(merged.size());
        int from = Math.min((safePageNum - 1) * safePageSize, merged.size());
        int to = Math.min(from + safePageSize, merged.size());
        if (from < to) {
            paging.setData(new ArrayList<>(merged.subList(from, to)));
        }
        return paging;
    }

    /**
     * 合并 DATA blob 并按条件过滤后统计条数
     *
     * @param rows   原始行集合
     * @param filter 过滤条件(可空)
     * @return 命中条数
     */
    public static long countDetails(Collection<?> rows, Predicate<Map<String, Object>> filter) {
        return mergeAll(rows, filter).size();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> mergeAll(Collection<?> rows, Predicate<Map<String, Object>> filter) {
        List<Map<String, Object>> merged = new ArrayList<>();
        if (rows == null) {
            return merged;
        }
        for (Object o : rows) {
            if (!(o instanceof Map)) {
                continue;
            }
            Map<String, Object> row = mergeDetailRow(new HashMap<>((Map<String, Object>) o));
            if (filter == null || filter.test(row)) {
                merged.add(row);
            }
        }
        return merged;
    }
}

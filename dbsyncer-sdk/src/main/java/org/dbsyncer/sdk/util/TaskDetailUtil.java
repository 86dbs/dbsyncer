/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.JsonUtil;
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
 * <p>明细按任务分表(每个任务一张表)，精简为固定列；校验/迁移明细的结构化字段
 * 统一序列化进 DATA blob(JSON)，读取时再还原到行 Map，供上层 VO/前端使用。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-17 18:00
 */
public abstract class TaskDetailUtil {

    private TaskDetailUtil() {
    }

    /**
     * 结构化内容序列化为 DATA blob 字节
     *
     * @param content 结构化字段
     * @return blob 字节，内容为空时返回 null
     */
    public static byte[] serializeContent(Map<String, Object> content) {
        if (content == null || content.isEmpty()) {
            return null;
        }
        return JsonUtil.objToJsonSafe(content).getBytes(StandardCharsets.UTF_8);
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
     * 明细分表查询后的统一后处理：合并 DATA blob → 应用过滤 → 排序 → 分页。
     * <p>校验/迁移的差异数、失败数等结构化指标存在 DATA blob 中，无法在库侧过滤排序，统一在应用侧处理。
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
            Map<String, Object> row = mergeDetailRow((Map<String, Object>) o);
            if (filter == null || filter.test(row)) {
                merged.add(row);
            }
        }
        return merged;
    }
}

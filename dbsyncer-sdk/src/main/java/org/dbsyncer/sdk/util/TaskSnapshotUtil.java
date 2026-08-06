/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.CommonTaskSnapshot;
import org.dbsyncer.sdk.model.DatabaseSyncTableSnapshot;

import com.alibaba.fastjson2.TypeReference;

import java.util.HashMap;
import java.util.Map;

/**
 * 运行快照与 Meta.SNAPSHOT 互转（权威在 {@code dbsyncer_meta}，不维护 Task 内存树）。
 * <p>任务级（IS_TASK_DETAIL=0）：整库迁移库映射 status 摘要（{@link ConfigConstant#META_SNAPSHOT_DATABASE}）；
 * 明细 Meta（TASK_ID=table_group.id）：单表续跑快照（{@link ConfigConstant#META_SNAPSHOT_TABLE_ONE}）。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-22 16:00
 */
public final class TaskSnapshotUtil {

    private TaskSnapshotUtil() {
    }

    /**
     * 任务级：写入/更新单个库映射 status。
     */
    public static Map<String, String> putMappingStatus(Map<String, String> metaSnapshot, int mappingIndex, int status) {
        Map<String, String> target = ensureMap(metaSnapshot);
        Map<String, Map<String, Object>> summary = readMappingStatusMap(target);
        Map<String, Object> one = new HashMap<>();
        one.put("status", status);
        summary.put(String.valueOf(mappingIndex), one);
        target.put(ConfigConstant.META_SNAPSHOT_DATABASE, JsonUtil.objToJson(summary));
        return target;
    }

    /**
     * 任务级：读取单个库映射 status（无则 null）。
     */
    public static Integer getMappingStatus(Map<String, String> metaSnapshot, int mappingIndex) {
        Map<String, Map<String, Object>> summary = readMappingStatusMap(metaSnapshot);
        Map<String, Object> one = summary.get(String.valueOf(mappingIndex));
        if (one == null || one.get("status") == null) {
            return null;
        }
        Object status = one.get("status");
        if (status instanceof Number) {
            return ((Number) status).intValue();
        }
        try {
            return Integer.valueOf(String.valueOf(status).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 任务级：库映射是否已完成。
     */
    public static boolean isMappingDone(Map<String, String> metaSnapshot, int mappingIndex) {
        return CommonTaskStatusEnum.isDone(getMappingStatus(metaSnapshot, mappingIndex));
    }

    /**
     * 任务级：清空库映射 status 摘要。
     */
    public static Map<String, String> clearMappingStatusSummary(Map<String, String> metaSnapshot) {
        Map<String, String> target = ensureMap(metaSnapshot);
        target.put(ConfigConstant.META_SNAPSHOT_DATABASE, "{}");
        return target;
    }

    /**
     * 进度明细：写入单表快照到结果 Meta.SNAPSHOT。
     *
     * @param mappingIndex 整库迁移时传入库映射 index；校验传 null
     */
    public static Map<String, String> writeTableSnapshot(Map<String, String> metaSnapshot,
                                                         CommonTaskSnapshot tableSnapshot,
                                                         Integer mappingIndex) {
        Map<String, String> target = ensureMap(metaSnapshot);
        if (tableSnapshot == null) {
            target.remove(ConfigConstant.META_SNAPSHOT_TABLE_ONE);
            target.remove(ConfigConstant.META_SNAPSHOT_MAPPING_INDEX);
            return target;
        }
        target.put(ConfigConstant.META_SNAPSHOT_TABLE_ONE, JsonUtil.objToJson(tableSnapshot));
        if (mappingIndex != null) {
            target.put(ConfigConstant.META_SNAPSHOT_MAPPING_INDEX, String.valueOf(mappingIndex));
        } else {
            target.remove(ConfigConstant.META_SNAPSHOT_MAPPING_INDEX);
        }
        return target;
    }

    /**
     * 进度明细：读取单表快照。
     */
    public static CommonTaskSnapshot readTableSnapshot(Map<String, String> metaSnapshot) {
        if (metaSnapshot == null) {
            return null;
        }
        String json = metaSnapshot.get(ConfigConstant.META_SNAPSHOT_TABLE_ONE);
        if (StringUtil.isBlank(json) || StringUtil.equals("{}", json.trim())) {
            return null;
        }
        DatabaseSyncTableSnapshot tableSnap = JsonUtil.jsonToObj(json, DatabaseSyncTableSnapshot.class);
        if (tableSnap != null) {
            return tableSnap;
        }
        return JsonUtil.jsonToObj(json, CommonTaskSnapshot.class);
    }

    /**
     * 进度明细：读取库映射 index（无则 null）。
     */
    public static Integer readMappingIndex(Map<String, String> metaSnapshot) {
        if (metaSnapshot == null) {
            return null;
        }
        return parseIndex(metaSnapshot.get(ConfigConstant.META_SNAPSHOT_MAPPING_INDEX));
    }

    public static boolean hasDatabaseSnapshots(Map<String, String> metaSnapshot) {
        return hasNonEmptyJson(metaSnapshot, ConfigConstant.META_SNAPSHOT_DATABASE);
    }

    public static boolean hasTableSnapshot(Map<String, String> metaSnapshot) {
        return hasNonEmptyJson(metaSnapshot, ConfigConstant.META_SNAPSHOT_TABLE_ONE);
    }

    /**
     * 解析任务级库映射 status 摘要为 index → status。
     */
    public static Map<Integer, Integer> readMappingStatusCodes(Map<String, String> metaSnapshot) {
        Map<Integer, Integer> result = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : readMappingStatusMap(metaSnapshot).entrySet()) {
            Integer index = parseIndex(entry.getKey());
            if (index == null || entry.getValue() == null || entry.getValue().get("status") == null) {
                continue;
            }
            Object status = entry.getValue().get("status");
            if (status instanceof Number) {
                result.put(index, ((Number) status).intValue());
            } else {
                try {
                    result.put(index, Integer.valueOf(String.valueOf(status).trim()));
                } catch (NumberFormatException ignored) {
                    // skip
                }
            }
        }
        return result;
    }

    private static Map<String, Map<String, Object>> readMappingStatusMap(Map<String, String> metaSnapshot) {
        if (metaSnapshot == null) {
            return new HashMap<>();
        }
        String json = metaSnapshot.get(ConfigConstant.META_SNAPSHOT_DATABASE);
        if (StringUtil.isBlank(json) || StringUtil.equals("{}", json.trim())) {
            return new HashMap<>();
        }
        Map<String, Map<String, Object>> parsed = JsonUtil.jsonToObj(json,
                new TypeReference<Map<String, Map<String, Object>>>() {
                });
        return parsed == null ? new HashMap<>() : new HashMap<>(parsed);
    }

    private static boolean hasNonEmptyJson(Map<String, String> metaSnapshot, String key) {
        if (metaSnapshot == null) {
            return false;
        }
        String json = metaSnapshot.get(key);
        return StringUtil.isNotBlank(json) && !StringUtil.equals("{}", json.trim());
    }

    private static Map<String, String> ensureMap(Map<String, String> metaSnapshot) {
        return metaSnapshot == null ? new HashMap<>() : metaSnapshot;
    }

    private static Integer parseIndex(String key) {
        if (StringUtil.isBlank(key)) {
            return null;
        }
        try {
            return Integer.valueOf(key.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

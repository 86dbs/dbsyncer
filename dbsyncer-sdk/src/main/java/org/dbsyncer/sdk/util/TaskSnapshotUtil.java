/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import com.alibaba.fastjson2.TypeReference;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.CommonTaskSnapshot;
import org.dbsyncer.sdk.model.DatabaseSyncSnapshot;
import org.dbsyncer.sdk.model.DatabaseSyncTableSnapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 运行快照与 Meta.SNAPSHOT 互转。
 * <p>任务级（IS_TASK_DETAIL=0）：整库迁移库映射 status 摘要（{@link ConfigConstant#META_SNAPSHOT_DATABASE}）；
 * 结果 Meta（TASK_ID=detail.id）：单表续跑快照（{@link ConfigConstant#META_SNAPSHOT_TABLE_ONE}）。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-22 16:00
 */
public final class TaskSnapshotUtil {

    private TaskSnapshotUtil() {
    }

    /**
     * 任务级：写入库映射 status 摘要（不含 tables）。
     */
    public static Map<String, String> writeDatabaseStatusSummary(Map<String, String> metaSnapshot,
                                                                 ConcurrentHashMap<Integer, DatabaseSyncSnapshot> snapshots) {
        Map<String, String> target = ensureMap(metaSnapshot);
        if (CollectionUtils.isEmpty(snapshots)) {
            target.put(ConfigConstant.META_SNAPSHOT_DATABASE, "{}");
            return target;
        }
        Map<String, Map<String, Object>> summary = new HashMap<>();
        for (Map.Entry<Integer, DatabaseSyncSnapshot> entry : snapshots.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            Map<String, Object> one = new HashMap<>();
            one.put("status", entry.getValue().getStatus());
            summary.put(String.valueOf(entry.getKey()), one);
        }
        target.put(ConfigConstant.META_SNAPSHOT_DATABASE, JsonUtil.objToJson(summary));
        return target;
    }

    /**
     * 任务级：读库映射 status 摘要到内存（保留已有 tables，仅补齐/更新 status）。
     */
    public static void readDatabaseStatusSummary(Map<String, String> metaSnapshot,
                                                 ConcurrentHashMap<Integer, DatabaseSyncSnapshot> target) {
        if (target == null || metaSnapshot == null) {
            return;
        }
        String json = metaSnapshot.get(ConfigConstant.META_SNAPSHOT_DATABASE);
        if (StringUtil.isBlank(json) || StringUtil.equals("{}", json.trim())) {
            return;
        }
        Map<String, DatabaseSyncSnapshot> parsed = JsonUtil.jsonToObj(json,
                new TypeReference<Map<String, DatabaseSyncSnapshot>>() {
                });
        if (parsed == null) {
            return;
        }
        for (Map.Entry<String, DatabaseSyncSnapshot> entry : parsed.entrySet()) {
            Integer index = parseIndex(entry.getKey());
            if (index == null || entry.getValue() == null) {
                continue;
            }
            DatabaseSyncSnapshot existing = target.get(index);
            if (existing == null) {
                DatabaseSyncSnapshot created = new DatabaseSyncSnapshot();
                created.setStatus(entry.getValue().getStatus());
                target.put(index, created);
            } else {
                existing.setStatus(entry.getValue().getStatus());
            }
        }
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

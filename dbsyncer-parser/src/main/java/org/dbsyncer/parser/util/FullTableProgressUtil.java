/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import com.alibaba.fastjson2.TypeReference;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.TableSyncProgress;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 全量同步表级进度读写（Meta.snapshot.tableProgress）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-11
 */
public abstract class FullTableProgressUtil {

    private FullTableProgressUtil() {
    }

    /**
     * 从 snapshot 加载表进度；无数据时返回空 Map（可写）。
     *
     * @param snapshot Meta.snapshot
     * @return tableGroupId -> 进度
     */
    public static Map<String, TableSyncProgress> load(Map<String, String> snapshot) {
        if (snapshot == null) {
            return new LinkedHashMap<>();
        }
        String json = snapshot.get(ParserEnum.TABLE_PROGRESS.getCode());
        if (StringUtil.isBlank(json)) {
            return new LinkedHashMap<>();
        }
        Map<String, TableSyncProgress> map = JsonUtil.jsonToObj(json, new TypeReference<Map<String, TableSyncProgress>>() {
        });
        if (map == null || map.isEmpty()) {
            return new LinkedHashMap<>();
        }
        return new LinkedHashMap<>(map);
    }

    /**
     * 写回表进度；空则移除 key。
     *
     * @param snapshot Meta.snapshot
     * @param progress 表进度
     */
    public static void save(Map<String, String> snapshot, Map<String, TableSyncProgress> progress) {
        if (snapshot == null) {
            return;
        }
        if (progress == null || progress.isEmpty()) {
            snapshot.remove(ParserEnum.TABLE_PROGRESS.getCode());
            return;
        }
        snapshot.put(ParserEnum.TABLE_PROGRESS.getCode(), JsonUtil.objToJson(progress));
    }

    /**
     * 更新单表进度并写回 snapshot。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @param progress     进度
     */
    public static void put(Map<String, String> snapshot, String tableGroupId, TableSyncProgress progress) {
        if (snapshot == null || StringUtil.isBlank(tableGroupId) || progress == null) {
            return;
        }
        Map<String, TableSyncProgress> map = load(snapshot);
        map.put(tableGroupId, progress);
        save(snapshot, map);
    }

    /**
     * 清除表进度。
     *
     * @param snapshot Meta.snapshot
     */
    public static void clear(Map<String, String> snapshot) {
        if (snapshot != null) {
            snapshot.remove(ParserEnum.TABLE_PROGRESS.getCode());
        }
    }

    /**
     * 是否不存在任何表进度。
     *
     * @param snapshot Meta.snapshot
     * @return true 表示无 tableProgress
     */
    public static boolean isEmpty(Map<String, String> snapshot) {
        return load(snapshot).isEmpty();
    }

    /**
     * 指定表是否已完成。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @return true 已完成
     */
    public static boolean isDone(Map<String, String> snapshot, String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return false;
        }
        TableSyncProgress progress = load(snapshot).get(tableGroupId);
        return progress != null && progress.isDone();
    }

    /**
     * 是否存在未完成的表。
     *
     * @param snapshot Meta.snapshot
     * @return true 存在未 done 的表
     */
    public static boolean hasIncomplete(Map<String, String> snapshot) {
        Map<String, TableSyncProgress> map = load(snapshot);
        if (CollectionUtils.isEmpty(map)) {
            return false;
        }
        for (TableSyncProgress progress : map.values()) {
            if (progress != null && !progress.isDone()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取或初始化单表进度（不写回）。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @return 进度，不会为 null
     */
    public static TableSyncProgress getOrInit(Map<String, String> snapshot, String tableGroupId) {
        Map<String, TableSyncProgress> map = load(snapshot);
        TableSyncProgress progress = map.get(tableGroupId);
        if (progress != null) {
            return progress;
        }
        TableSyncProgress created = new TableSyncProgress();
        created.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
        created.setCursor(StringUtil.EMPTY);
        created.setDone(false);
        return created;
    }
}

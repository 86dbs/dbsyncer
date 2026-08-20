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
import org.dbsyncer.sdk.model.WorkItemIds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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
     * 仅当 incoming 相对 current 单调前进时写入；否则保持原状。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @param incoming     候选进度
     * @return true 已写入；false 被拒绝（过期/回退）
     */
    public static boolean putIfMonotonic(Map<String, String> snapshot, String tableGroupId, TableSyncProgress incoming) {
        if (snapshot == null || StringUtil.isBlank(tableGroupId) || incoming == null) {
            return false;
        }
        Map<String, TableSyncProgress> map = load(snapshot);
        TableSyncProgress current = map.get(tableGroupId);
        if (!isMonotonicAdvance(current, incoming)) {
            return false;
        }
        map.put(tableGroupId, incoming);
        save(snapshot, map);
        return true;
    }

    /**
     * 判断候选进度是否相对已有进度单调前进（禁止游标回退与过期 generation 覆盖）。
     *
     * @param current  已有进度，可为 null
     * @param incoming 候选进度
     * @return true 允许覆盖
     */
    public static boolean isMonotonicAdvance(TableSyncProgress current, TableSyncProgress incoming) {
        if (incoming == null) {
            return false;
        }
        if (current == null) {
            return true;
        }
        if (current.getGeneration() > 0 && incoming.getGeneration() > 0
                && incoming.getGeneration() < current.getGeneration()) {
            return false;
        }
        if (current.isDone()) {
            return incoming.isDone();
        }
        if (incoming.isDone()) {
            return true;
        }
        if (incoming.getPageIndex() > current.getPageIndex()) {
            return true;
        }
        if (incoming.getPageIndex() < current.getPageIndex()) {
            return false;
        }
        String oldCursor = StringUtil.getIfBlank(current.getCursor(), StringUtil.EMPTY);
        String newCursor = StringUtil.getIfBlank(incoming.getCursor(), StringUtil.EMPTY);
        if (StringUtil.equals(oldCursor, newCursor)) {
            return incoming.getGeneration() >= current.getGeneration();
        }
        // 同页游标变化：要求非空推进（空游标不能覆盖已有游标）
        if (StringUtil.isBlank(newCursor) && StringUtil.isNotBlank(oldCursor)) {
            return false;
        }
        return true;
    }

    /**
     * 游标/页码是否相对已有进度严格前进（仅此时允许累加 success/fail）。
     * <p>同水位仅升高 generation 不算前进，避免切主改派后同一页再计一次。
     *
     * @param current  已有进度，可为 null
     * @param incoming 候选进度
     * @return true 可计增量
     */
    public static boolean isStrictlyAhead(TableSyncProgress current, TableSyncProgress incoming) {
        if (incoming == null) {
            return false;
        }
        if (current == null) {
            return true;
        }
        if (current.isDone() || incoming.isDone()) {
            return false;
        }
        if (incoming.getPageIndex() > current.getPageIndex()) {
            return true;
        }
        if (incoming.getPageIndex() < current.getPageIndex()) {
            return false;
        }
        String oldCursor = StringUtil.getIfBlank(current.getCursor(), StringUtil.EMPTY);
        String newCursor = StringUtil.getIfBlank(incoming.getCursor(), StringUtil.EMPTY);
        if (StringUtil.equals(oldCursor, newCursor)) {
            return false;
        }
        if (StringUtil.isBlank(newCursor) && StringUtil.isNotBlank(oldCursor)) {
            return false;
        }
        return true;
    }

    /**
     * 列出某表已落盘的 range 进度 key（不含整表 key）。
     * <p>切主后若 tableRangePlan 丢失，可用此列表避免按新节点数重切导致 itemId 变化、整表重计。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @return range itemId，按字典序稳定排序
     */
    public static List<String> listRangeProgressKeys(Map<String, String> snapshot, String tableGroupId) {
        List<String> result = new ArrayList<>();
        if (StringUtil.isBlank(tableGroupId)) {
            return result;
        }
        for (String key : load(snapshot).keySet()) {
            if (WorkItemIds.isShard(key) && WorkItemIds.belongsToTable(key, tableGroupId)) {
                result.add(key);
            }
        }
        Collections.sort(result);
        return result;
    }

    /**
     * 清除表进度。
     *
     * @param snapshot Meta.snapshot
     */
    public static void clear(Map<String, String> snapshot) {
        if (snapshot != null) {
            snapshot.remove(ParserEnum.TABLE_PROGRESS.getCode());
            snapshot.remove(ParserEnum.TABLE_RANGE_PLAN.getCode());
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
     * 指定进度 key（整表或 range itemId）是否已完成。
     *
     * @param snapshot Meta.snapshot
     * @param itemId   进度 key
     * @return true 已完成
     */
    public static boolean isDone(Map<String, String> snapshot, String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return false;
        }
        TableSyncProgress progress = load(snapshot).get(itemId);
        return progress != null && progress.isDone();
    }

    /**
     * 表是否已全部完成：有 range 计划则计划内全部 done；否则看整表 key。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @return true 完成
     */
    public static boolean isTableFullyDone(Map<String, String> snapshot, String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return false;
        }
        List<String> plan = getRangePlan(snapshot, tableGroupId);
        if (!CollectionUtils.isEmpty(plan)) {
            for (String itemId : plan) {
                if (!isDone(snapshot, itemId)) {
                    return false;
                }
            }
            return true;
        }
        TableSyncProgress whole = load(snapshot).get(tableGroupId);
        return whole != null && whole.isDone();
    }

    /**
     * 保存/覆盖某表的 range 计划。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @param itemIds      完整 range item 列表
     */
    public static void putRangePlan(Map<String, String> snapshot, String tableGroupId, List<String> itemIds) {
        if (snapshot == null || StringUtil.isBlank(tableGroupId) || CollectionUtils.isEmpty(itemIds)) {
            return;
        }
        Map<String, List<String>> plans = loadRangePlans(snapshot);
        plans.put(tableGroupId, new ArrayList<>(itemIds));
        snapshot.put(ParserEnum.TABLE_RANGE_PLAN.getCode(), JsonUtil.objToJson(plans));
    }

    /**
     * 读取某表 range 计划。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @return itemId 列表，可能为空
     */
    public static List<String> getRangePlan(Map<String, String> snapshot, String tableGroupId) {
        if (snapshot == null || StringUtil.isBlank(tableGroupId)) {
            return Collections.emptyList();
        }
        List<String> plan = loadRangePlans(snapshot).get(tableGroupId);
        return plan == null ? Collections.emptyList() : plan;
    }

    /**
     * 加载全部 range 计划。
     *
     * @param snapshot Meta.snapshot
     * @return tableGroupId -> itemIds
     */
    public static Map<String, List<String>> loadRangePlans(Map<String, String> snapshot) {
        if (snapshot == null) {
            return new LinkedHashMap<>();
        }
        String json = snapshot.get(ParserEnum.TABLE_RANGE_PLAN.getCode());
        if (StringUtil.isBlank(json)) {
            return new LinkedHashMap<>();
        }
        Map<String, List<String>> map = JsonUtil.jsonToObj(json, new TypeReference<Map<String, List<String>>>() {
        });
        if (map == null || map.isEmpty()) {
            return new LinkedHashMap<>();
        }
        return new LinkedHashMap<>(map);
    }

    /**
     * 列出某表未完成的进度 key（优先按 range 计划）。
     *
     * @param snapshot     Meta.snapshot
     * @param tableGroupId 表映射 ID
     * @return 未完成 itemId
     */
    public static List<String> listIncompleteItems(Map<String, String> snapshot, String tableGroupId) {
        List<String> result = new ArrayList<>();
        if (StringUtil.isBlank(tableGroupId)) {
            return result;
        }
        List<String> plan = getRangePlan(snapshot, tableGroupId);
        if (!CollectionUtils.isEmpty(plan)) {
            for (String itemId : plan) {
                if (!isDone(snapshot, itemId)) {
                    result.add(itemId);
                }
            }
            return result;
        }
        if (!isDone(snapshot, tableGroupId)) {
            result.add(tableGroupId);
        }
        return result;
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

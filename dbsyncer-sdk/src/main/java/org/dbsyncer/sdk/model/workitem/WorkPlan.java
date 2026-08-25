/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.workitem;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 表内工作项计划（Leader 规划结果）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-25
 */
public final class WorkPlan {

    private final List<WorkBound> bounds;

    public WorkPlan(List<WorkBound> bounds) {
        if (CollectionUtils.isEmpty(bounds)) {
            this.bounds = Collections.emptyList();
        } else {
            this.bounds = Collections.unmodifiableList(new ArrayList<>(bounds));
        }
    }

    /**
     * 空计划（调用方按整表处理）。
     *
     * @return 空计划
     */
    public static WorkPlan wholeTable() {
        return new WorkPlan(Collections.emptyList());
    }

    /**
     * 单表整项计划。
     *
     * @param tableGroupId 表映射 ID
     * @return 计划
     */
    public static WorkPlan wholeTable(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return wholeTable();
        }
        return new WorkPlan(Collections.singletonList(WorkBound.whole(tableGroupId)));
    }

    /**
     * 由边界列表构造。
     *
     * @param bounds 边界列表
     * @return 计划
     */
    public static WorkPlan of(List<WorkBound> bounds) {
        return new WorkPlan(bounds);
    }

    public List<WorkBound> getBounds() {
        return bounds;
    }

    /**
     * 是否拆成多个带边界的工作项。
     *
     * @return true 已拆分
     */
    public boolean isSplit() {
        if (bounds.size() <= 1) {
            return false;
        }
        for (WorkBound bound : bounds) {
            if (bound != null && !bound.isWhole()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 全部工作项 ID。
     *
     * @return itemId 列表
     */
    public List<String> itemIds() {
        if (bounds.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> ids = new ArrayList<>(bounds.size());
        for (WorkBound bound : bounds) {
            if (bound != null && StringUtil.isNotBlank(bound.getItemId())) {
                ids.add(bound.getItemId());
            }
        }
        return ids;
    }

    /**
     * 按 itemId 查找边界。
     *
     * @param itemId 工作项 ID
     * @return 边界；未找到为 null
     */
    public WorkBound findByItemId(String itemId) {
        if (StringUtil.isBlank(itemId)) {
            return null;
        }
        for (WorkBound bound : bounds) {
            if (bound != null && StringUtil.equals(itemId, bound.getItemId())) {
                return bound;
            }
        }
        return null;
    }
}

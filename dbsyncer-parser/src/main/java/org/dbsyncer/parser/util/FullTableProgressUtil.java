/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.model.CommonTaskSnapshot;
import org.dbsyncer.sdk.util.TaskSnapshotUtil;

import java.util.List;
import java.util.Map;

/**
 * 全量同步表级进度：
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-11
 */
public abstract class FullTableProgressUtil {

    private FullTableProgressUtil() {
    }

    /**
     * 解析表级 Meta。
     *
     * @param metaProfile  Meta 服务
     * @param tableGroupId 表映射 ID
     * @return 明细 Meta，可能为 null
     */
    public static Meta resolve(MetaProfile metaProfile, String tableGroupId) {
        if (metaProfile == null || StringUtil.isBlank(tableGroupId)) {
            return null;
        }
        return metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
    }

    /**
     * 表是否已完成
     *
     * @param metaProfile  Meta 服务
     * @param tableGroupId 表映射 ID
     * @return true 已完成
     */
    public static boolean isDone(MetaProfile metaProfile, String tableGroupId) {
        Meta meta = resolve(metaProfile, tableGroupId);
        if (meta == null) {
            return false;
        }
        if (CommonTaskStatusEnum.isDone(meta.getState())) {
            return true;
        }
        CommonTaskSnapshot snap = TaskSnapshotUtil.readTableSnapshot(meta.getSnapshot());
        return snap != null && CommonTaskStatusEnum.isDone(snap.getStatus());
    }

    /**
     * 读取或初始化单表进度（不写库）。
     *
     * @param metaProfile  Meta 服务
     * @param tableGroupId 表映射 ID
     * @return 进度，不会为 null
     */
    public static CommonTaskSnapshot getOrInit(MetaProfile metaProfile, String tableGroupId) {
        Meta meta = resolve(metaProfile, tableGroupId);
        if (meta != null) {
            CommonTaskSnapshot snap = TaskSnapshotUtil.readTableSnapshot(meta.getSnapshot());
            if (snap != null) {
                return snap;
            }
        }
        return readySnapshot();
    }

    /**
     * 覆盖写单表进度到明细 Meta（无则创建）。
     * <p>已存在时经 {@link MetaProfile#updateMetaProgress} 写前重载，避免覆盖原子计数。
     *
     * @param profileComponent 配置写入口（新建走 addConfigModel）
     * @param metaProfile      Meta 服务
     * @param tableGroupId     表映射 ID
     * @param snapshot         表级快照；null 表示清空快照且 state=READY
     */
    public static void save(ProfileComponent profileComponent, MetaProfile metaProfile,
                            String tableGroupId, CommonTaskSnapshot snapshot) {
        if (metaProfile == null || StringUtil.isBlank(tableGroupId)) {
            return;
        }
        synchronized (tableLock(tableGroupId)) {
            Meta meta = resolve(metaProfile, tableGroupId);
            long now = System.currentTimeMillis();
            if (meta == null || StringUtil.isBlank(meta.getId())) {
                meta = new Meta();
                meta.setTaskId(tableGroupId);
                meta.setIsTaskDetail(TaskLevelEnum.TASK_DETAIL.getCode());
                meta.setCreateTime(now);
                apply(meta, snapshot, now);
                if (profileComponent != null) {
                    profileComponent.addConfigModel(meta);
                } else {
                    metaProfile.addMeta(meta);
                }
                return;
            }
            Map<String, String> newSnap = TaskSnapshotUtil.writeTableSnapshot(meta.getSnapshot(), snapshot, null);
            int state = snapshot == null
                    ? CommonTaskStatusEnum.READY.getCode()
                    : snapshot.getStatus();
            metaProfile.updateMetaProgress(meta.getId(), state, newSnap);
        }
    }

    /**
     * 清空任务下全部表明细进度（快照清空、state=READY；保留 success/fail 等计数）。
     *
     * @param profileComponent 配置写入口
     * @param metaProfile      Meta 服务
     * @param tableGroupIds    表映射 ID 列表
     */
    public static void clearAll(ProfileComponent profileComponent, MetaProfile metaProfile, List<String> tableGroupIds) {
        if (profileComponent == null || metaProfile == null || CollectionUtils.isEmpty(tableGroupIds)) {
            return;
        }
        for (String tableGroupId : tableGroupIds) {
            if (StringUtil.isBlank(tableGroupId)) {
                continue;
            }
            save(profileComponent, metaProfile, tableGroupId, null);
        }
    }

    /**
     * 是否存在未完成的表进度（有快照且非 DONE，或 state 为运行中/停止中）。
     *
     * @param metaProfile   Meta 服务
     * @param tableGroupIds 表映射 ID 列表
     * @return true 存在未完成进度
     */
    public static boolean hasIncomplete(MetaProfile metaProfile, List<String> tableGroupIds) {
        if (metaProfile == null || CollectionUtils.isEmpty(tableGroupIds)) {
            return false;
        }
        for (String tableGroupId : tableGroupIds) {
            if (StringUtil.isBlank(tableGroupId) || isDone(metaProfile, tableGroupId)) {
                continue;
            }
            Meta meta = resolve(metaProfile, tableGroupId);
            if (meta == null) {
                continue;
            }
            if (CommonTaskStatusEnum.isRunning(meta.getState())) {
                return true;
            }
            CommonTaskSnapshot snap = TaskSnapshotUtil.readTableSnapshot(meta.getSnapshot());
            if (snap == null) {
                continue;
            }
            if (CommonTaskStatusEnum.isRunning(snap.getStatus())
                    || snap.getPageIndex() > ParserEnum.PAGE_INDEX.getDefaultValue()
                    || StringUtil.isNotBlank(snap.getCursor())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 构造运行中快照。
     *
     * @param pageIndex 页码
     * @param cursor    游标
     * @return 快照
     */
    public static CommonTaskSnapshot runningSnapshot(int pageIndex, String cursor) {
        CommonTaskSnapshot snap = new CommonTaskSnapshot();
        snap.setStatus(CommonTaskStatusEnum.RUNNING.getCode());
        snap.setPageIndex(pageIndex > 0 ? pageIndex : ParserEnum.PAGE_INDEX.getDefaultValue());
        snap.setCursor(StringUtil.getIfBlank(cursor, StringUtil.EMPTY));
        return snap;
    }

    /**
     * 构造已完成快照。
     *
     * @return 快照
     */
    public static CommonTaskSnapshot doneSnapshot() {
        CommonTaskSnapshot snap = new CommonTaskSnapshot();
        snap.setStatus(CommonTaskStatusEnum.DONE.getCode());
        snap.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
        snap.setCursor(StringUtil.EMPTY);
        return snap;
    }

    /**
     * 表级写锁（与 FlushStrategyImpl 表明细计数增量共用同一键）。
     *
     * @param tableGroupId 表映射 ID
     * @return 锁对象
     */
    public static Object tableLock(String tableGroupId) {
        String id = StringUtil.isBlank(tableGroupId) ? StringUtil.EMPTY : tableGroupId;
        return ("table-meta-" + id).intern();
    }

    private static CommonTaskSnapshot readySnapshot() {
        CommonTaskSnapshot snap = new CommonTaskSnapshot();
        snap.setStatus(CommonTaskStatusEnum.READY.getCode());
        snap.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
        snap.setCursor(StringUtil.EMPTY);
        return snap;
    }

    private static void apply(Meta meta, CommonTaskSnapshot snapshot, long now) {
        Map<String, String> snap = TaskSnapshotUtil.writeTableSnapshot(meta.getSnapshot(), snapshot, null);
        meta.setSnapshot(snap);
        if (snapshot == null) {
            meta.setState(CommonTaskStatusEnum.READY.getCode());
        } else {
            meta.setState(snapshot.getStatus());
        }
        meta.setUpdateTime(now);
    }
}

/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.BatchTaskUtil;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.AbstractPuller;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.event.FullRefreshEvent;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.TableSyncProgress;
import org.dbsyncer.parser.model.Task;
import org.dbsyncer.parser.util.FullTableProgressUtil;
import org.dbsyncer.parser.util.MetaLockUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * 全量同步（表级并发，对齐数据迁移：threadNum 控制表并发，单表内读写串行）。
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2020-04-26 15:28
 */
@Component
public final class FullPuller extends AbstractPuller implements ApplicationListener<FullRefreshEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 单表内写批同步执行器（不再使用 threadNum 做写并发）
     */
    private static final Executor SYNC_WRITE_EXECUTOR = Runnable::run;

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private LogService logService;

    private final Map<String, Task> map = new ConcurrentHashMap<>();

    @Override
    public void start(Mapping mapping) {
        Thread worker = new Thread(() -> runSync(mapping, true));
        worker.setName("full-worker-" + mapping.getId());
        worker.setDaemon(false);
        worker.start();
    }

    /**
     * 同步执行全量任务
     *
     * @param mapping       驱动
     * @param publishClosed 完成后是否发布关闭事件
     */
    public void runSync(Mapping mapping, boolean publishClosed) {
        Assert.isTrue(tableGroupProfile.getTableGroupCount(mapping.getId()) > 0, "映射关系不能为空");
        final String metaId = mapping.getMetaId();
        try {
            Task task = map.computeIfAbsent(metaId, k -> new Task(metaId));
            logger.info("开始全量同步：{}, {}", metaId, mapping.getName());
            doTask(task, mapping);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logService.log(LogType.SystemLog.ERROR, e.getMessage());
        } finally {
            map.remove(metaId);
            if (publishClosed) {
                publishClosedEvent(metaId);
            }
            logger.info("结束全量同步：{}, {}", metaId, mapping.getName());
        }
    }

    @Override
    public void close(String metaId) {
        map.computeIfPresent(metaId, (k, task) -> {
            task.stop();
            return null;
        });
    }

    @Override
    public void onApplicationEvent(FullRefreshEvent event) {
        flush(event.getTask());
    }

    private void doTask(Task task, Mapping mapping) {
        long now = Instant.now().toEpochMilli();
        task.setBeginTime(now);
        task.setEndTime(now);

        Meta meta = metaProfile.getMeta(task.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        // 旧版单游标断点：绝对表序下标（用于无 tableProgress 时跳过已完成表）
        int legacyTableGroupIndex = NumberUtil.toInt(snapshot.get(ParserEnum.TABLE_GROUP_INDEX.getCode()),
                ParserEnum.TABLE_GROUP_INDEX.getDefaultValue());
        int legacyPageIndex = NumberUtil.toInt(snapshot.get(ParserEnum.PAGE_INDEX.getCode()),
                ParserEnum.PAGE_INDEX.getDefaultValue());
        String legacyCursor = snapshot.get(ParserEnum.CURSOR.getCode());
        boolean useLegacyBreakpoint = FullTableProgressUtil.isEmpty(snapshot)
                && (legacyTableGroupIndex > ParserEnum.TABLE_GROUP_INDEX.getDefaultValue()
                || legacyPageIndex > ParserEnum.PAGE_INDEX.getDefaultValue()
                || StringUtil.isNotBlank(legacyCursor));

        flush(task);

        int pageSize = ConfigConstant.PAGE_SIZE;
        int pageNum = 1;
        while (task.isRunning()) {
            Paging<TableGroup> paging = tableGroupProfile.queryTableGroup(mapping.getId(), null, pageNum, pageSize);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<TableGroup> page = new ArrayList<>(paging.getData());
            int pageStartIndex = (pageNum - 1) * pageSize;
            List<TableWork> works = new ArrayList<>(page.size());
            for (int j = 0; j < page.size(); j++) {
                works.add(new TableWork(pageStartIndex + j, page.get(j)));
            }

            int threadNum = Math.max(1, mapping.getThreadNum());
            BatchTaskUtil.executeBySlice(works, works.size(), threadNum, (slice, executor) ->
                    BatchTaskUtil.executeWithAwait(slice, executor, work ->
                            syncOneTable(task, mapping, work, useLegacyBreakpoint, legacyTableGroupIndex,
                                    legacyPageIndex, legacyCursor), logger), logger);

            if (!task.isRunning() || page.size() < pageSize) {
                break;
            }
            pageNum++;
        }

        task.setEndTime(Instant.now().toEpochMilli());
        task.setTableGroupIndex(ParserEnum.TABLE_GROUP_INDEX.getDefaultValue());
        task.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
        task.setCursors(null);
        if (task.isRunning()) {
            clearFullProgress(task);
        } else {
            flush(task);
        }
    }

    private void syncOneTable(Task parent, Mapping mapping, TableWork work, boolean useLegacyBreakpoint,
                              int legacyTableGroupIndex, int legacyPageIndex, String legacyCursor) {
        if (!parent.isRunning()) {
            return;
        }
        TableGroup tableGroup = work.tableGroup;
        String tableGroupId = tableGroup.getId();
        int absoluteIndex = work.absoluteIndex;

        Meta meta = metaProfile.getMeta(parent.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        if (FullTableProgressUtil.isDone(snapshot, tableGroupId)) {
            return;
        }
        // 旧断点：绝对下标之前的表视为已完成并跳过
        if (useLegacyBreakpoint && absoluteIndex < legacyTableGroupIndex) {
            markTableDone(parent, tableGroupId);
            return;
        }

        Task tableTask = parent.createTableTask(tableGroupId);
        TableSyncProgress progress = FullTableProgressUtil.getOrInit(snapshot, tableGroupId);
        if (useLegacyBreakpoint && absoluteIndex == legacyTableGroupIndex
                && progress.getPageIndex() == ParserEnum.PAGE_INDEX.getDefaultValue()
                && StringUtil.isBlank(progress.getCursor())) {
            tableTask.setPageIndex(legacyPageIndex);
            tableTask.setCursors(PrimaryKeyUtil.getLastCursors(legacyCursor));
        } else {
            tableTask.setPageIndex(progress.getPageIndex() > 0 ? progress.getPageIndex() : ParserEnum.PAGE_INDEX.getDefaultValue());
            tableTask.setCursors(PrimaryKeyUtil.getLastCursors(progress.getCursor()));
        }

        try {
            parserComponent.execute(tableTask, mapping, tableGroup, SYNC_WRITE_EXECUTOR);
            if (parent.isRunning()) {
                markTableDone(parent, tableGroupId);
            }
        } catch (Exception e) {
            logger.error("全量同步表失败: {} -> {}, {}",
                    tableGroup.getSourceTable() != null ? tableGroup.getSourceTable().getName() : tableGroupId,
                    tableGroup.getTargetTable() != null ? tableGroup.getTargetTable().getName() : tableGroupId,
                    e.getMessage(), e);
            logService.log(LogType.SystemLog.ERROR, e.getMessage());
        }
    }

    private void flush(Task task) {
        synchronized (metaLock(task.getId())) {
            Meta meta = metaProfile.getMeta(task.getId());
            Assert.notNull(meta, "检查meta为空.");
            refreshMetaTotals(meta, task);

            Task root = task.getParent() != null ? task.getParent() : task;
            meta.setBeginTime(root.getBeginTime());
            meta.setEndTime(root.getEndTime());
            meta.setUpdateTime(Instant.now().toEpochMilli());
            Map<String, String> snapshot = meta.getSnapshot();

            if (StringUtil.isNotBlank(task.getTableGroupId())) {
                TableSyncProgress progress = new TableSyncProgress();
                progress.setPageIndex(task.getPageIndex());
                progress.setCursor(StringUtil.getIfBlank(StringUtil.join(task.getCursors(), StringUtil.COMMA), StringUtil.EMPTY));
                progress.setDone(false);
                FullTableProgressUtil.put(snapshot, task.getTableGroupId(), progress);
                snapshot.put(ParserEnum.PAGE_INDEX.getCode(), String.valueOf(task.getPageIndex()));
                snapshot.put(ParserEnum.CURSOR.getCode(), progress.getCursor());
            } else {
                snapshot.put(ParserEnum.PAGE_INDEX.getCode(), String.valueOf(task.getPageIndex()));
                snapshot.put(ParserEnum.CURSOR.getCode(),
                        StringUtil.getIfBlank(StringUtil.join(task.getCursors(), StringUtil.COMMA), StringUtil.EMPTY));
                snapshot.put(ParserEnum.TABLE_GROUP_INDEX.getCode(), String.valueOf(task.getTableGroupIndex()));
            }
            profileComponent.editConfigModel(meta);
        }
    }

    private void markTableDone(Task parent, String tableGroupId) {
        synchronized (metaLock(parent.getId())) {
            Meta meta = metaProfile.getMeta(parent.getId());
            Assert.notNull(meta, "检查meta为空.");
            TableSyncProgress progress = new TableSyncProgress();
            progress.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
            progress.setCursor(StringUtil.EMPTY);
            progress.setDone(true);
            FullTableProgressUtil.put(meta.getSnapshot(), tableGroupId, progress);
            refreshMetaTotals(meta, parent);
            meta.setUpdateTime(Instant.now().toEpochMilli());
            profileComponent.editConfigModel(meta);
        }
    }

    private void clearFullProgress(Task task) {
        synchronized (metaLock(task.getId())) {
            Meta meta = metaProfile.getMeta(task.getId());
            Assert.notNull(meta, "检查meta为空.");
            refreshMetaTotals(meta, task);
            meta.setBeginTime(task.getBeginTime());
            meta.setEndTime(task.getEndTime());
            meta.setUpdateTime(Instant.now().toEpochMilli());
            Map<String, String> snapshot = meta.getSnapshot();
            FullTableProgressUtil.clear(snapshot);
            snapshot.put(ParserEnum.PAGE_INDEX.getCode(), String.valueOf(ParserEnum.PAGE_INDEX.getDefaultValue()));
            snapshot.put(ParserEnum.CURSOR.getCode(), StringUtil.EMPTY);
            snapshot.put(ParserEnum.TABLE_GROUP_INDEX.getCode(),
                    String.valueOf(ParserEnum.TABLE_GROUP_INDEX.getDefaultValue()));
            profileComponent.editConfigModel(meta);
        }
    }

    private void refreshMetaTotals(Meta meta, Task task) {
        long finished = meta.getSuccess().get() + meta.getFail().get();
        if (meta.getTotal().get() < finished) {
            meta.getTotal().set(finished);
        }
        Task root = task.getParent() != null ? task.getParent() : task;
        meta.setBeginTime(root.getBeginTime());
        meta.setEndTime(root.getEndTime());
    }

    private Object metaLock(String metaId) {
        return MetaLockUtil.lock(metaId);
    }

    /**
     * 带绝对下标的表任务（用于旧断点兼容）。
     */
    private static final class TableWork {
        private final int absoluteIndex;
        private final TableGroup tableGroup;

        private TableWork(int absoluteIndex, TableGroup tableGroup) {
            this.absoluteIndex = absoluteIndex;
            this.tableGroup = tableGroup;
        }
    }
}

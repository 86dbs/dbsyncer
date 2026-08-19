/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.model.Result;
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
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.sdk.model.WorkItemAssignment;
import org.dbsyncer.sdk.model.WorkItemIds;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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

    private static final long CLUSTER_WAIT_MS = 2000L;

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

    @Resource
    private ClusterService clusterService;

    private final Map<String, Task> map = new ConcurrentHashMap<>();

    @Override
    public void start(Mapping mapping) {
        boolean publishClosed = clusterService.isStandalone();
        Thread worker = new Thread(() -> runSync(mapping, publishClosed));
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
    public boolean isActive(String metaId) {
        return map.containsKey(metaId);
    }

    @Override
    public void onApplicationEvent(FullRefreshEvent event) {
        if (event == null || event.getTask() == null) {
            return;
        }
        boolean committed = flush(event.getTask(), event.getResult());
        event.setProgressCommitted(committed);
    }

    private void doTask(Task task, Mapping mapping) {
        Meta meta = metaProfile.getMeta(task.getId());
        Assert.notNull(meta, "检查meta为空.");
        long now = Instant.now().toEpochMilli();
        // 切主/本节点重拉起时保留原 beginTime，避免耗时被重置、整行回写冲掉 SUCCESS/SNAPSHOT
        long beginTime = meta.getBeginTime() > 0L ? meta.getBeginTime() : now;
        task.setBeginTime(beginTime);
        task.setEndTime(now);
        if (meta.getBeginTime() <= 0L) {
            metaProfile.ensureStartTime(task.getId(), beginTime);
        }

        Map<String, String> snapshot = meta.getSnapshot();
        int legacyTableGroupIndex = NumberUtil.toInt(snapshot.get(ParserEnum.TABLE_GROUP_INDEX.getCode()),
                ParserEnum.TABLE_GROUP_INDEX.getDefaultValue());
        int legacyPageIndex = NumberUtil.toInt(snapshot.get(ParserEnum.PAGE_INDEX.getCode()),
                ParserEnum.PAGE_INDEX.getDefaultValue());
        String legacyCursor = snapshot.get(ParserEnum.CURSOR.getCode());
        boolean useLegacyBreakpoint = FullTableProgressUtil.isEmpty(snapshot)
                && (legacyTableGroupIndex > ParserEnum.TABLE_GROUP_INDEX.getDefaultValue()
                || legacyPageIndex > ParserEnum.PAGE_INDEX.getDefaultValue()
                || StringUtil.isNotBlank(legacyCursor));

        if (clusterService.isStandalone()) {
            scanAssignedPages(task, mapping, useLegacyBreakpoint, legacyTableGroupIndex, legacyPageIndex, legacyCursor);
        } else {
            runUntilClusterComplete(task, mapping, useLegacyBreakpoint, legacyTableGroupIndex, legacyPageIndex,
                    legacyCursor);
        }
        finishTask(task);
    }

    private void runUntilClusterComplete(Task task, Mapping mapping, boolean useLegacyBreakpoint,
                                         int legacyTableGroupIndex, int legacyPageIndex, String legacyCursor) {
        while (task.isRunning()) {
            scanAssignedPages(task, mapping, useLegacyBreakpoint, legacyTableGroupIndex, legacyPageIndex, legacyCursor);
            if (!task.isRunning() || clusterService.areAllTablesDone(mapping.getId())) {
                return;
            }
            sleepWait(task);
        }
    }

    private void scanAssignedPages(Task task, Mapping mapping, boolean useLegacyBreakpoint, int legacyTableGroupIndex,
                                   int legacyPageIndex, String legacyCursor) {
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
            pageNum++;
        }
    }

    private void sleepWait(Task task) {
        try {
            Thread.sleep(CLUSTER_WAIT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            task.stop();
        }
    }

    private void finishTask(Task task) {
        task.setEndTime(Instant.now().toEpochMilli());
        task.setTableGroupIndex(ParserEnum.TABLE_GROUP_INDEX.getDefaultValue());
        task.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
        task.setCursors(null);
        if (task.isRunning() && clusterService.isStandalone()) {
            clearFullProgress(task);
        }
        // 停止时不再整行 editConfigModel：会覆盖并发 SUCCESS/SNAPSHOT（切主/停节点双计根因之一）
    }

    private void syncOneTable(Task parent, Mapping mapping, TableWork work, boolean useLegacyBreakpoint,
                              int legacyTableGroupIndex, int legacyPageIndex, String legacyCursor) {
        if (!parent.isRunning()) {
            return;
        }
        TableGroup tableGroup = work.tableGroup;
        String tableGroupId = tableGroup.getId();
        List<String> itemIds = resolveLocalItems(tableGroupId);
        if (CollectionUtils.isEmpty(itemIds)) {
            return;
        }
        int absoluteIndex = work.absoluteIndex;

        Meta meta = metaProfile.getMeta(parent.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        if (FullTableProgressUtil.isTableFullyDone(snapshot, tableGroupId)) {
            return;
        }
        // 旧断点：绝对下标之前的表视为已完成并跳过（仅整表语义）
        if (useLegacyBreakpoint && absoluteIndex < legacyTableGroupIndex
                && itemIds.size() == 1 && StringUtil.equals(itemIds.get(0), tableGroupId)) {
            markItemDone(parent, tableGroupId, tableGroupId);
            return;
        }

        for (String itemId : itemIds) {
            if (!parent.isRunning()) {
                return;
            }
            syncOneItem(parent, mapping, tableGroup, itemId, useLegacyBreakpoint, absoluteIndex,
                    legacyTableGroupIndex, legacyPageIndex, legacyCursor);
        }
    }

    private void syncOneItem(Task parent, Mapping mapping, TableGroup tableGroup, String itemId,
                             boolean useLegacyBreakpoint, int absoluteIndex, int legacyTableGroupIndex,
                             int legacyPageIndex, String legacyCursor) {
        String tableGroupId = tableGroup.getId();
        Meta meta = metaProfile.getMeta(parent.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        if (FullTableProgressUtil.isDone(snapshot, itemId)) {
            return;
        }

        TableGroup execGroup = prepareExecTableGroup(mapping, tableGroup, itemId);
        Task tableTask = parent.createTableTask(itemId);
        TableSyncProgress progress = FullTableProgressUtil.getOrInit(snapshot, itemId);
        if (useLegacyBreakpoint && StringUtil.equals(itemId, tableGroupId)
                && absoluteIndex == legacyTableGroupIndex
                && progress.getPageIndex() == ParserEnum.PAGE_INDEX.getDefaultValue()
                && StringUtil.isBlank(progress.getCursor())) {
            tableTask.setPageIndex(legacyPageIndex);
            tableTask.setCursors(PrimaryKeyUtil.getLastCursors(legacyCursor));
        } else {
            tableTask.setPageIndex(progress.getPageIndex() > 0 ? progress.getPageIndex() : ParserEnum.PAGE_INDEX.getDefaultValue());
            tableTask.setCursors(PrimaryKeyUtil.getLastCursors(progress.getCursor()));
        }

        try {
            if (!clusterService.assertWritable(itemId)) {
                logger.warn("generation 围栏失效，停止写表: {}", itemId);
                return;
            }
            boolean completed = parserComponent.execute(tableTask, mapping, execGroup, SYNC_WRITE_EXECUTOR);
            if (completed && parent.isRunning()) {
                markItemDone(parent, itemId, tableGroupId);
            }
        } catch (Exception e) {
            logger.error("全量同步表失败: {} -> {}, item={}, {}",
                    tableGroup.getSourceTable() != null ? tableGroup.getSourceTable().getName() : tableGroupId,
                    tableGroup.getTargetTable() != null ? tableGroup.getTargetTable().getName() : tableGroupId,
                    itemId, e.getMessage(), e);
            logService.log(LogType.SystemLog.ERROR, e.getMessage());
        }
    }

    private List<String> resolveLocalItems(String tableGroupId) {
        if (clusterService.isStandalone()) {
            return Collections.singletonList(tableGroupId);
        }
        List<String> items = new ArrayList<>();
        for (WorkItemAssignment assignment : clusterService.listLocalAssignments()) {
            if (assignment != null && WorkItemIds.belongsToTable(assignment.getItemId(), tableGroupId)) {
                items.add(assignment.getItemId());
            }
        }
        return items;
    }

    private TableGroup prepareExecTableGroup(Mapping mapping, TableGroup source, String itemId) {
        WorkItemIds.Range range = WorkItemIds.parse(itemId);
        if (range == null) {
            return source;
        }
        List<String> pks = PrimaryKeyUtil.findTablePrimaryKeys(source.getSourceTable());
        if (CollectionUtils.isEmpty(pks) || pks.size() != 1) {
            return source;
        }
        TableGroup copy = copyTableGroup(source);
        List<Filter> filters = new ArrayList<>();
        if (!CollectionUtils.isEmpty(source.getFilter())) {
            for (Filter filter : source.getFilter()) {
                if (filter == null) {
                    continue;
                }
                Filter cloned = new Filter();
                cloned.setName(filter.getName());
                cloned.setOperation(filter.getOperation());
                cloned.setFilter(filter.getFilter());
                cloned.setValue(filter.getValue());
                filters.add(cloned);
            }
        }
        filters.add(buildPkBoundFilter(pks.get(0), FilterEnum.GT_AND_EQUAL.getName(), String.valueOf(range.getFromInclusive())));
        filters.add(buildPkBoundFilter(pks.get(0), FilterEnum.LT_AND_EQUAL.getName(), String.valueOf(range.getToInclusive())));
        copy.setFilter(filters);
        copy.setCommand(parserComponent.getCommand(mapping, copy));
        return copy;
    }

    private static Filter buildPkBoundFilter(String pkName, String filterOp, String value) {
        Filter filter = new Filter();
        filter.setName(pkName);
        filter.setOperation(OperationEnum.AND.getName());
        filter.setFilter(filterOp);
        filter.setValue(value);
        return filter;
    }

    private static TableGroup copyTableGroup(TableGroup source) {
        TableGroup copy = new TableGroup();
        copy.setId(source.getId());
        copy.setTaskId(source.getTaskId());
        copy.setIndex(source.getIndex());
        copy.setSourceConnectorId(source.getSourceConnectorId());
        copy.setTargetConnectorId(source.getTargetConnectorId());
        copy.setSourceDatabase(source.getSourceDatabase());
        copy.setTargetDatabase(source.getTargetDatabase());
        copy.setSourceSchema(source.getSourceSchema());
        copy.setTargetSchema(source.getTargetSchema());
        copy.setSourceTable(source.getSourceTable());
        copy.setTargetTable(source.getTargetTable());
        copy.setFieldMapping(source.getFieldMapping());
        copy.setPlugin(source.getPlugin());
        copy.setPluginExtInfo(source.getPluginExtInfo());
        copy.setConvert(source.getConvert());
        return copy;
    }

    private boolean flush(Task task, Result result) {
        synchronized (metaLock(task.getId())) {
            String itemId = task.getTableGroupId();
            if (StringUtil.isNotBlank(itemId) && !clusterService.assertWritable(itemId)) {
                logger.warn("generation 围栏失效，跳过进度与计数: {}", itemId);
                return false;
            }
            Meta meta = metaProfile.getMeta(task.getId());
            Assert.notNull(meta, "检查meta为空.");
            refreshMetaTotals(meta, task);

            if (StringUtil.isNotBlank(itemId)) {
                TableSyncProgress progress = new TableSyncProgress();
                progress.setPageIndex(task.getPageIndex());
                progress.setCursor(StringUtil.getIfBlank(StringUtil.join(task.getCursors(), StringUtil.COMMA), StringUtil.EMPTY));
                progress.setDone(false);
                progress.setGeneration(clusterService.getLocalGeneration(itemId));
                long successDelta = result == null || result.getSuccessData() == null ? 0L : result.getSuccessData().size();
                long failDelta = result == null || result.getFailData() == null ? 0L : result.getFailData().size();
                if (!metaProfile.mergeTableProgress(task.getId(), itemId, progress, successDelta, failDelta)) {
                    logger.warn("进度非单调或 CAS 冲突，跳过计数: {}", itemId);
                    return false;
                }
                commitTableMetaCount(result);
                return true;
            }

            // 无工作项时不做整行回写，避免覆盖并发 SUCCESS / tableProgress
            logger.debug("跳过无 itemId 的 Meta 整行 flush, metaId={}", task.getId());
            return true;
        }
    }

    private void commitTableMetaCount(Result result) {
        if (result == null) {
            return;
        }
        int success = result.getSuccessData() == null ? 0 : result.getSuccessData().size();
        int fail = result.getFailData() == null ? 0 : result.getFailData().size();
        if (success == 0 && fail == 0 || StringUtil.isBlank(result.getTableGroupId())) {
            return;
        }
        String tableGroupId = result.getTableGroupId();
        Meta tableMeta = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (tableMeta == null) {
            tableMeta = new Meta();
            tableMeta.setTaskId(tableGroupId);
            tableMeta.setIsTaskDetail(TaskLevelEnum.TASK_DETAIL.getCode());
            long now = Instant.now().toEpochMilli();
            tableMeta.setCreateTime(now);
            tableMeta.setUpdateTime(now);
            profileComponent.addConfigModel(tableMeta);
            tableMeta = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        }
        if (tableMeta != null && StringUtil.isNotBlank(tableMeta.getId())) {
            metaProfile.incrementMeta(MetaIncrement.of(tableMeta.getId()).success(success).fail(fail));
        }
    }

    private void markItemDone(Task parent, String itemId, String tableGroupId) {
        synchronized (metaLock(parent.getId())) {
            if (!clusterService.assertWritable(itemId)) {
                logger.warn("generation 围栏失效，跳过完成标记: {}", itemId);
                return;
            }
            TableSyncProgress progress = new TableSyncProgress();
            progress.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
            progress.setCursor(StringUtil.EMPTY);
            progress.setDone(true);
            progress.setGeneration(clusterService.getLocalGeneration(itemId));
            if (!metaProfile.mergeTableProgress(parent.getId(), itemId, progress)) {
                logger.warn("完成标记被拒绝: {}", itemId);
                return;
            }
            Meta meta = metaProfile.getMeta(parent.getId());
            if (meta != null) {
                refreshMetaTotals(meta, parent);
            }
        }
        Meta latest = metaProfile.getMeta(parent.getId());
        if (latest != null && FullTableProgressUtil.isTableFullyDone(latest.getSnapshot(), tableGroupId)) {
            markTableDetailDone(tableGroupId);
        }
    }

    private void markTableDetailDone(String tableGroupId) {
        Meta detail = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (detail == null || detail.getState() == CommonTaskStatusEnum.DONE.getCode()) {
            return;
        }
        metaProfile.updateMetaState(detail.getId(), CommonTaskStatusEnum.DONE.getCode());
    }

    private void clearFullProgress(Task task) {
        synchronized (metaLock(task.getId())) {
            Meta meta = metaProfile.getMeta(task.getId());
            Assert.notNull(meta, "检查meta为空.");
            refreshMetaTotals(meta, task, true);
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
        refreshMetaTotals(meta, task, false);
    }

    private void refreshMetaTotals(Meta meta, Task task, boolean completed) {
        long finished = meta.getSuccess().get() + meta.getFail().get();
        if (completed) {
            // COUNT(*) 仅作运行中预估；结束后以实际处理条数为准，避免进度卡在 99.x%
            if (finished > 0) {
                meta.getTotal().set(finished);
            }
        } else if (meta.getTotal().get() < finished) {
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

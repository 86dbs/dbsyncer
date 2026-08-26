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
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.WorkItemIds;
import org.dbsyncer.sdk.model.workitem.WorkBound;
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

    /**
     * 单切片连续失败退避上限。
     */
    private static final long ITEM_FAIL_BACKOFF_MAX_MS = 60_000L;

    /**
     * 单切片首次失败退避。
     */
    private static final long ITEM_FAIL_BACKOFF_BASE_MS = 1_000L;

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

    /**
     * itemId -> 连续失败退避状态（进程内，重启清空）。
     */
    private final Map<String, ItemFailState> itemFailStates = new ConcurrentHashMap<>();

    @Override
    public void start(Mapping mapping) {
        Thread worker = new Thread(() -> runSync(mapping, clusterService.isStandalone()));
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
            if (publishClosed || shouldPublishClosedAfterStop(metaId)) {
                publishClosedEvent(metaId);
            }
            logger.info("结束全量同步：{}, {}", metaId, mapping.getName());
        }
    }

    /**
     * 集群下正常跑完由集群控制面收口置 READY；
     * 用户停止后 Meta 为 STOPPING，Worker 退出时须发 ClosedEvent，否则会一直停在「停止中」。
     */
    private boolean shouldPublishClosedAfterStop(String metaId) {
        if (StringUtil.isBlank(metaId) || clusterService.isStandalone()) {
            return false;
        }
        Meta meta = metaProfile.getMeta(metaId);
        return meta != null && meta.getState() == CommonTaskStatusEnum.STOPPING.getCode();
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
        if (event.getTask() == null) {
            return;
        }
        boolean committed = flush(event.getTask(), event.getResult());
        event.setProgressCommitted(committed);
    }

    private void doTask(Task task, Mapping mapping) {
        Meta meta = metaProfile.getMeta(task.getId());
        Assert.notNull(meta, "检查meta为空.");
        long now = Instant.now().toEpochMilli();
        // 切主/本节点重拉起时保留原 startTime，避免耗时被重置、整行回写冲掉 SUCCESS/SNAPSHOT
        long startTime = meta.getStartTime() > 0L ? meta.getStartTime() : now;
        task.setStartTime(startTime);
        task.setEndTime(now);
        if (meta.getStartTime() <= 0L) {
            metaProfile.ensureStartTime(task.getId(), startTime);
        }
        if (clusterService.isStandalone()) {
            scanAssignedPages(task, mapping);
        } else {
            runUntilClusterComplete(task, mapping);
        }
        finishTask(task);
    }

    private void runUntilClusterComplete(Task task, Mapping mapping) {
        while (task.isRunning()) {
            scanAssignedPages(task, mapping);
            if (!task.isRunning() || clusterService.areAllTablesDone(mapping.getId())) {
                return;
            }
            sleepWait(task);
        }
    }

    private void scanAssignedPages(Task task, Mapping mapping) {
        int pageSize = ConfigConstant.PAGE_SIZE;
        int pageNum = 1;
        while (task.isRunning()) {
            Paging<TableGroup> paging = tableGroupProfile.queryTableGroup(mapping.getId(), null, pageNum, pageSize);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<TableGroup> page = new ArrayList<>(paging.getData());
            List<TableWork> works = new ArrayList<>(page.size());
            for (TableGroup group : page) {
                works.add(new TableWork(group));
            }
            int threadNum = Math.max(1, mapping.getThreadNum());
            BatchTaskUtil.executeBySlice(works, works.size(), threadNum, (slice, executor) ->
                    BatchTaskUtil.executeWithAwait(slice, executor, work ->
                            syncOneTable(task, mapping, work), logger), logger);
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
        task.setCursors(null);
        if (task.isRunning() && clusterService.isStandalone()) {
            clearFullProgress(task);
        }
    }

    private void syncOneTable(Task parent, Mapping mapping, TableWork work) {
        if (!parent.isRunning()) {
            return;
        }
        TableGroup tableGroup = work.tableGroup;
        String tableGroupId = tableGroup.getId();
        //获取本机WorkItems
        List<String> itemIds = clusterService.resolveLocalWorkItems(tableGroupId);
        if (CollectionUtils.isEmpty(itemIds)) {
            return;
        }

        Meta meta = metaProfile.getMeta(parent.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        if (FullTableProgressUtil.isTableFullyDone(snapshot, tableGroupId)) {
            return;
        }

        for (String itemId : itemIds) {
            if (!parent.isRunning()) {
                return;
            }
            syncOneItem(parent, mapping, tableGroup, itemId);
        }
    }

    private void syncOneItem(Task parent, Mapping mapping, TableGroup tableGroup, String itemId) {
        String tableGroupId = tableGroup.getId();
        Meta meta = metaProfile.getMeta(parent.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        if (FullTableProgressUtil.isDone(snapshot, itemId)) {
            return;
        }

        TableGroup execGroup = tableGroup;
        Task tableTask = parent.createTableTask(itemId);
        TableSyncProgress progress = FullTableProgressUtil.getOrInit(snapshot, itemId);
        tableTask.setPageIndex(progress.getPageIndex() > 0 ? progress.getPageIndex() : ParserEnum.PAGE_INDEX.getDefaultValue());
        tableTask.setCursors(resolveItemStartCursors(itemId, progress.getCursor(), tableGroup.getSourceTable()));
        tableTask.setProcessed(Math.max(0L, progress.getProcessed()));

        try {
            if (!awaitItemRetryWindow(parent, itemId)) {
                return;
            }
            if (!clusterService.assertWritable(itemId)) {
                logger.warn("generation 围栏失效，停止写表: {}", itemId);
                return;
            }
            boolean completed = parserComponent.execute(tableTask, mapping, execGroup, SYNC_WRITE_EXECUTOR);
            if (completed && parent.isRunning()) {
                clearItemFailState(itemId);
                markItemDone(parent, itemId, tableGroupId);
            }
        } catch (Exception e) {
            ItemFailState failState = onItemFail(itemId);
            logger.error("全量同步表失败: {} -> {}, item={}, consecutive={}, nextRetryMs={}, {}",
                    tableGroup.getSourceTable() != null ? tableGroup.getSourceTable().getName() : tableGroupId,
                    tableGroup.getTargetTable() != null ? tableGroup.getTargetTable().getName() : tableGroupId,
                    itemId, failState.consecutive, Math.max(0L, failState.nextRetryAtMs - System.currentTimeMillis()),
                    e.getMessage(), e);
            logService.log(LogType.SystemLog.ERROR, e.getMessage());
        }
    }

    /**
     * 项内续跑用进度游标；无进度时用 CURSOR_BATCH 的排他起始游标（走 pk &gt; from 分页）。
     */
    private static Object[] resolveItemStartCursors(String itemId, String progressCursor, Table sourceTable) {
        int expectedPkCount = countSupportedCursorPks(sourceTable);
        Object[] cursors = PrimaryKeyUtil.getLastCursors(progressCursor);
        if (cursors != null && cursors.length > 0) {
            assertCursorColumnCount(itemId, cursors, expectedPkCount);
            return cursors;
        }
        if (!WorkItemIds.isCursorBatch(itemId)) {
            return cursors;
        }
        WorkBound bound = WorkItemIds.toWorkBound(itemId);
        if (bound == null) {
            return cursors;
        }
        String from = bound.getFrom();
        if (StringUtil.isBlank(from)) {
            return cursors;
        }
        Object[] fromCursors = PrimaryKeyUtil.getLastCursors(from);
        if (fromCursors == null || fromCursors.length == 0) {
            return cursors;
        }
        assertCursorColumnCount(itemId, fromCursors, expectedPkCount);
        return fromCursors;
    }

    private static int countSupportedCursorPks(Table sourceTable) {
        if (sourceTable == null) {
            return 0;
        }
        List<Field> pkFields = PrimaryKeyUtil.findPrimaryKeyFields(sourceTable.getColumn());
        if (CollectionUtils.isEmpty(pkFields) || !PrimaryKeyUtil.isSupportedCursor(pkFields)) {
            return 0;
        }
        return pkFields.size();
    }

    private static void assertCursorColumnCount(String itemId, Object[] cursors, int expectedPkCount) {
        if (expectedPkCount > 0 && cursors != null && cursors.length != expectedPkCount) {
            throw new IllegalStateException(String.format(
                    "CURSOR_BATCH 起始游标列数不匹配 itemId=%s, expected=%d, actual=%d",
                    itemId, expectedPkCount, cursors.length));
        }
    }

    /**
     * 失败切片退避：未到重试时间则等待（可被停止打断）。
     *
     * @return false 任务已停止
     */
    private boolean awaitItemRetryWindow(Task parent, String itemId) {
        for (;;) {
            if (!parent.isRunning()) {
                return false;
            }
            ItemFailState state = itemFailStates.get(itemId);
            if (state == null) {
                return true;
            }
            long waitMs = state.nextRetryAtMs - System.currentTimeMillis();
            if (waitMs <= 0L) {
                return true;
            }
            try {
                Thread.sleep(Math.min(waitMs, 1000L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                parent.stop();
                return false;
            }
        }
    }

    private ItemFailState onItemFail(String itemId) {
        ItemFailState state = itemFailStates.computeIfAbsent(itemId, k -> new ItemFailState());
        state.consecutive++;
        int shift = Math.min(Math.max(state.consecutive - 1, 0), 5);
        long delay = Math.min(ITEM_FAIL_BACKOFF_MAX_MS, ITEM_FAIL_BACKOFF_BASE_MS << shift);
        state.nextRetryAtMs = System.currentTimeMillis() + delay;
        return state;
    }

    private void clearItemFailState(String itemId) {
        if (StringUtil.isNotBlank(itemId)) {
            itemFailStates.remove(itemId);
        }
    }

    private static final class ItemFailState {
        private int consecutive;
        private long nextRetryAtMs;
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
                progress.setProcessed(Math.max(0L, task.getProcessed()));
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
            progress.setProcessed(0L);
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
            meta.setStartTime(task.getStartTime());
            meta.setUpdateTime(Instant.now().toEpochMilli());
            Map<String, String> snapshot = meta.getSnapshot();
            FullTableProgressUtil.clear(snapshot);
            FullTableProgressUtil.removeLegacyTaskBreakpointKeys(snapshot);
            profileComponent.editConfigModel(meta);
        }
    }

    private void refreshMetaTotals(Meta meta, Task task) {
        refreshMetaTotals(meta, task, false);
    }

    private void refreshMetaTotals(Meta meta, Task task, boolean completed) {
        long finished = meta.getSuccess().get() + meta.getFail().get();
        long total = meta.getTotal().get();
        if (completed) {
            // 仅抬升：禁止把总数改小（切片提前结束时会把 6000万改成 1000万掩盖漏数）
            if (finished > total) {
                meta.getTotal().set(finished);
            }
        } else if (total < finished) {
            meta.getTotal().set(finished);
        }
        Task root = task.getParent() != null ? task.getParent() : task;
        meta.setStartTime(root.getStartTime());
        meta.setUpdateTime(root.getEndTime());
    }

    private Object metaLock(String metaId) {
        return MetaLockUtil.lock(metaId);
    }

    /**
     * 表任务包装。
     */
    private static final class TableWork {
        private final TableGroup tableGroup;

        private TableWork(TableGroup tableGroup) {
            this.tableGroup = tableGroup;
        }
    }
}

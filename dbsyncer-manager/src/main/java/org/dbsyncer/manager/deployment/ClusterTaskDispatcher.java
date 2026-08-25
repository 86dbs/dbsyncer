/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.deployment;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.util.FullTableProgressUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.LeaderLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * 按租约在本节点拉起/停掉 Mapping；升主时接管 RUNNING 任务。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
@Component
public class ClusterTaskDispatcher implements LeaderLifecycleListener, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ClusterService clusterService;

    @Resource
    private ManagerFactory managerFactory;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @PostConstruct
    public void init() {
        clusterService.addLeaderListener(this);
        scheduledTaskService.start("cluster-task-dispatch", 5000L, this);
    }

    @Override
    public void onLeaderStart(long term) {
        logger.info("升主 term={}，接管 RUNNING 任务", term);
        run();
    }

    @Override
    public void onLeaderStop(long term) {
        logger.info("卸任 term={}，停止本节点未持有租约的任务", term);
        run();
    }

    @Override
    public void run() {
        taskProfile.pageScanTasks(Mapping.class, ConfigConstant.PAGE_SIZE, mappings -> {
            for (Mapping mapping : mappings) {
                if (clusterService.isLeader()) {
                    reassign(mapping);
                    tryCompleteFull(mapping);
                }
                dispatchOne(mapping);
            }
        });
    }

    private void reassign(Mapping mapping) {
        if (mapping == null || StringUtil.isBlank(mapping.getMetaId())) {
            return;
        }
        Meta meta = metaProfile.getMeta(mapping.getMetaId());
        if (meta == null || meta.getState() != CommonTaskStatusEnum.RUNNING.getCode()) {
            // 未运行/已结束：清掉残留派工，避免集群页仍显示工作项/增量数
            clusterService.clearTaskAssignments(mapping.getId());
            return;
        }
        String model = mapping.getModel();
        if (ModelEnum.isIncrement(model) || isIncrementPhase(meta, model)) {
            clusterService.assignIncrementMapping(mapping.getId());
            return;
        }
        if (ModelEnum.isFull(model)) {
            clusterService.assignTableGroups(mapping.getId());
        }
    }

    private void dispatchOne(Mapping mapping) {
        if (mapping == null || StringUtil.isBlank(mapping.getMetaId())) {
            return;
        }
        String metaId = mapping.getMetaId();
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null || meta.getState() != CommonTaskStatusEnum.RUNNING.getCode()) {
            if (managerFactory.isLocalActive(metaId)) {
                managerFactory.stopLocal(mapping);
            } else if (meta != null && meta.getState() == CommonTaskStatusEnum.STOPPING.getCode()
                    && (clusterService.isStandalone() || clusterService.isLeader())) {
                // Worker 已退出但未发 ClosedEvent（历史版本/竞态）时，Leader 收口停止中
                managerFactory.changeMetaState(metaId, CommonTaskStatusEnum.READY);
            }
            return;
        }
        boolean shouldRun = shouldRunLocal(mapping, meta);
        boolean active = managerFactory.isLocalActive(metaId);
        if (shouldRun && !active) {
            tryStart(mapping, metaId);
            return;
        }
        if (!shouldRun && active) {
            managerFactory.stopLocal(mapping);
        }
    }

    private void tryStart(Mapping mapping, String metaId) {
        try {
            managerFactory.start(mapping, true);
        } catch (Exception e) {
            logger.debug("本节点未拉起同步任务 metaId={}, err={}", metaId, e.getMessage());
        }
    }

    private boolean shouldRunLocal(Mapping mapping, Meta meta) {
        if (clusterService.isStandalone()) {
            return true;
        }
        String model = mapping.getModel();
        if (ModelEnum.isIncrement(model) || isIncrementPhase(meta, model)) {
            return clusterService.isIncrementAssignedToLocal(mapping.getId());
        }
        if (clusterService.areAllTablesDone(mapping.getId())) {
            return false;
        }
        return FullTableProgressUtil.hasIncompleteLocalAssignment(
                mapping.getId(),
                meta == null ? null : meta.getSnapshot(),
                clusterService.listLocalAssignments())
                || managerFactory.isLocalActive(meta.getId());
    }

    private void tryCompleteFull(Mapping mapping) {
        if (mapping == null || StringUtil.isBlank(mapping.getMetaId()) || clusterService.isStandalone()) {
            return;
        }
        Meta meta = metaProfile.getMeta(mapping.getMetaId());
        if (meta == null || meta.getState() != CommonTaskStatusEnum.RUNNING.getCode()) {
            return;
        }
        String model = mapping.getModel();
        if (!ModelEnum.isFull(model) || isIncrementPhase(meta, model)) {
            return;
        }
        if (!clusterService.areAllTablesDone(mapping.getId())) {
            return;
        }
        metaProfile.alignMetaTotalToProcessed(mapping.getMetaId());
        if (StringUtil.equals(ModelEnum.FULLINCREMENT.getCode(), model)) {
            Meta latest = metaProfile.getMeta(mapping.getMetaId());
            markIncrementPhase(latest != null ? latest : meta);
            clusterService.assignIncrementMapping(mapping.getId());
            return;
        }
        clusterService.clearTaskAssignments(mapping.getId());
        managerFactory.changeMetaState(mapping.getMetaId(), CommonTaskStatusEnum.READY);
    }

    private void markIncrementPhase(Meta meta) {
        if (meta.getSnapshot() == null) {
            return;
        }
        meta.getSnapshot().put(ParserEnum.FULL_INCREMENT_PHASE.getCode(), ModelEnum.INCREMENT.getCode());
        FullTableProgressUtil.clear(meta.getSnapshot());
        FullTableProgressUtil.removeLegacyTaskBreakpointKeys(meta.getSnapshot());
        meta.setUpdateTime(System.currentTimeMillis());
        profileComponent.editConfigModel(meta);
    }

    private boolean isIncrementPhase(Meta meta, String model) {
        if (!StringUtil.equals(ModelEnum.FULLINCREMENT.getCode(), model)) {
            return false;
        }
        String phase = meta.getSnapshot() == null ? null
                : meta.getSnapshot().get(ParserEnum.FULL_INCREMENT_PHASE.getCode());
        return ModelEnum.isIncrement(phase);
    }
}

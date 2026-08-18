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
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
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
    private TableGroupProfile tableGroupProfile;

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
            return;
        }
        String model = mapping.getModel();
        if (ModelEnum.isFull(model) && !isIncrementPhase(meta, model)) {
            clusterService.assignTableGroups(mapping.getId());
        }
        if (ModelEnum.isIncrement(model) || isIncrementPhase(meta, model)) {
            clusterService.assignIncrementMapping(meta.getId());
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
                managerFactory.close(mapping);
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
            managerFactory.close(mapping);
        }
    }

    private void tryStart(Mapping mapping, String metaId) {
        try {
            managerFactory.start(mapping, true);
        } catch (Exception e) {
            logger.debug("本节点未拉起驱动 metaId={}, err={}", metaId, e.getMessage());
        }
    }

    private boolean shouldRunLocal(Mapping mapping, Meta meta) {
        if (clusterService.isStandalone()) {
            return true;
        }
        String model = mapping.getModel();
        if (ModelEnum.isIncrement(model) || isIncrementPhase(meta, model)) {
            return clusterService.hasValidLease(meta.getId());
        }
        return clusterService.isLeader() || hasAssignedTable(mapping.getId());
    }

    private boolean isIncrementPhase(Meta meta, String model) {
        if (!StringUtil.equals(ModelEnum.FULLINCREMENT.getCode(), model)) {
            return false;
        }
        String phase = meta.getSnapshot() == null ? null
                : meta.getSnapshot().get(ParserEnum.FULL_INCREMENT_PHASE.getCode());
        return ModelEnum.isIncrement(phase);
    }

    private boolean hasAssignedTable(String taskId) {
        final boolean[] hit = {false};
        tableGroupProfile.pageScanTableGroups(taskId, ConfigConstant.PAGE_SIZE, groups -> {
            for (TableGroup group : groups) {
                if (group != null && clusterService.isTableAssignedToLocal(group.getId())) {
                    hit[0] = true;
                }
            }
        });
        return hit[0];
    }
}

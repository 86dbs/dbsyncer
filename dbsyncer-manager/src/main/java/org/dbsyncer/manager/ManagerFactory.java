package org.dbsyncer.manager;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.event.ClosedEvent;
import org.dbsyncer.manager.impl.ConnectorInstanceBinder;
import org.dbsyncer.manager.impl.IncrementPuller;
import org.dbsyncer.parser.MappingRuntimeService;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.util.FullTableProgressUtil;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public class ManagerFactory implements MappingRuntimeService, ApplicationListener<ClosedEvent> {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private Map<String, Puller> map;

    @Resource
    private ClusterService clusterService;

    @Resource
    private ConnectorInstanceBinder connectorInstanceBinder;

    @Resource
    private IncrementPuller incrementPuller;

    @Override
    public void onApplicationEvent(ClosedEvent event) {
        changeMetaState(event.getMetaId(), CommonTaskStatusEnum.READY);
        Meta meta = metaProfile.getMeta(event.getMetaId());
        if (meta != null && StringUtil.isNotBlank(meta.getTaskId())) {
            clusterService.clearTaskSchedule(meta.getTaskId());
        }
    }

    public void start(Mapping mapping) {
        start(mapping, false);
    }

    /**
     * 启动驱动。
     *
     * @param mapping      驱动
     * @param autoRecovery 是否为服务重启自动恢复（true 时对 CDC 监听启动失败按配置重试）
     */
    @Override
    public void start(Mapping mapping, boolean autoRecovery) {
        Puller puller = getPuller(mapping);
        String metaId = mapping.getMetaId();
        if (puller.isActive(metaId) || clusterService.isShardOrchestrationActive(mapping.getId())) {
            return;
        }
        Meta current = metaProfile.getMeta(metaId);
        boolean alreadyRunning = current != null && current.getState() == CommonTaskStatusEnum.RUNNING.getCode();
        // 先置 RUNNING，再派工/通知：Leader 未分配扫描与远端 executeLocal 都以 Meta 为准，避免已停任务被误拉起
        changeMetaState(metaId, CommonTaskStatusEnum.RUNNING);
        boolean runLocal = clusterService.prepareTaskStart(mapping.getId(), mapping.getModel());
        if (!runLocal) {
            return;
        }
        try {
            startLocal(mapping, autoRecovery);
        } catch (Exception e) {
            if (!alreadyRunning) {
                changeMetaState(metaId, CommonTaskStatusEnum.READY);
                clusterService.clearTaskSchedule(mapping.getId());
            }
            throw new ManagerException(e.getMessage());
        }
    }

    @Override
    public void startLocal(Mapping mapping, boolean autoRecovery) {
        Puller puller = getPuller(mapping);
        String metaId = mapping.getMetaId();
        if (puller.isActive(metaId) || clusterService.isShardOrchestrationActive(mapping.getId())) {
            return;
        }
        if (!clusterService.assertTaskWritable(mapping.getId())) {
            return;
        }
        connectorInstanceBinder.bind(mapping);
        // 集群批处理全量可由分片编排接管，不再走整表 Puller
        if (clusterService.tryStartShardOrchestration(mapping.getId(), mapping.getModel())) {
            return;
        }
        puller.start(mapping, autoRecovery);
    }

    public void close(Mapping mapping) {
        Puller puller = getPuller(mapping);

        String metaId = mapping.getMetaId();
        changeMetaState(metaId, CommonTaskStatusEnum.STOPPING);

        clusterService.stopShardOrchestration(mapping.getId());
        puller.close(metaId);
        clusterService.clearTaskSchedule(mapping.getId());
    }

    /**
     * 仅停止本进程 Puller，不改调度行。
     *
     * @param mapping 驱动
     */
    @Override
    public void stopLocal(Mapping mapping) {
        clusterService.stopShardOrchestration(mapping.getId());
        Puller puller = getPuller(mapping);
        puller.close(mapping.getMetaId());
    }

    /**
     * 本进程是否已启动该驱动。
     *
     * @param metaId Meta ID
     * @return true 已启动
     */
    @Override
    public boolean isLocalActive(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta != null && clusterService.isShardOrchestrationActive(meta.getTaskId())) {
            return true;
        }
        for (Puller puller : map.values()) {
            if (puller.isActive(metaId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void prepareBatchFullPhase(Mapping mapping) {
        if (mapping == null || !StringUtil.equals(ModelEnum.FULLINCREMENT.getCode(), mapping.getModel())) {
            return;
        }
        Meta meta = metaProfile.getMeta(mapping.getMetaId());
        String phase = meta == null || meta.getSnapshot() == null
                ? null : meta.getSnapshot().get(ParserEnum.FULL_INCREMENT_PHASE.getCode());
        if (ModelEnum.isIncrement(phase)) {
            return;
        }
        incrementPuller.captureAndSaveOffset(mapping);
    }

    @Override
    public void startIncrementAfterBatchFull(Mapping mapping) {
        if (mapping == null || !StringUtil.equals(ModelEnum.FULLINCREMENT.getCode(), mapping.getModel())) {
            return;
        }
        String metaId = mapping.getMetaId();
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null) {
            return;
        }
        meta.getSnapshot().put(ParserEnum.FULL_INCREMENT_PHASE.getCode(), ModelEnum.INCREMENT.getCode());
        meta.getSnapshot().remove(ParserEnum.PAGE_INDEX.getCode());
        meta.getSnapshot().remove(ParserEnum.CURSOR.getCode());
        meta.getSnapshot().remove(ParserEnum.TABLE_GROUP_INDEX.getCode());
        meta.getSnapshot().remove("tableProgress");
        FullTableProgressUtil.clearAll(profileComponent, metaProfile, tableGroupProfile.listTableGroupIds(mapping.getId()));
        profileComponent.editConfigModel(meta);
        if (incrementPuller.isActive(metaId)) {
            return;
        }
        incrementPuller.start(mapping, false);
    }

    @Override
    public void changeMetaState(String metaId, CommonTaskStatusEnum status) {
        Meta meta = metaProfile.getMeta(metaId);
        int code = status.getCode();
        if (null != meta && meta.getState() != code) {
            meta.setState(code);
            meta.setUpdateTime(Instant.now().toEpochMilli());
            profileComponent.editConfigModel(meta);
        }
    }

    private Puller getPuller(Mapping mapping) {
        Assert.notNull(mapping, "驱动不能为空");
        String model = mapping.getModel();
        String metaId = mapping.getMetaId();
        Assert.hasText(model, "同步方式不能为空");
        Assert.hasText(metaId, "任务ID不能为空");

        Puller puller = map.get(model.concat("Puller"));
        Assert.notNull(puller, String.format("未知的同步方式: %s", model));
        return puller;
    }
}

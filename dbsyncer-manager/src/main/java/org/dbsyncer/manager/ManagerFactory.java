package org.dbsyncer.manager;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.event.ClosedEvent;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Lazy;
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
public class ManagerFactory implements ApplicationListener<ClosedEvent> {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private Map<String, Puller> map;

    @Resource
    private ClusterService clusterService;

    @Resource
    @Lazy
    private PreloadTemplate preloadTemplate;

    @Override
    public void onApplicationEvent(ClosedEvent event) {
        changeMetaState(event.getMetaId(), CommonTaskStatusEnum.READY);
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
    public void start(Mapping mapping, boolean autoRecovery) {
        Puller puller = getPuller(mapping);
        String metaId = mapping.getMetaId();
        if (puller.isActive(metaId)) {
            return;
        }
        prepareClusterStart(mapping);
        // Follower 本进程连接池没有 Mapping 级实例（连接只在 Leader 增改 Mapping 时建立）
        preloadTemplate.reConnect(mapping);

        Meta current = metaProfile.getMeta(metaId);
        boolean alreadyRunning = current != null && current.getState() == CommonTaskStatusEnum.RUNNING.getCode();
        changeMetaState(metaId, CommonTaskStatusEnum.RUNNING);

        try {
            puller.start(mapping, autoRecovery);
        } catch (Exception e) {
            if (!alreadyRunning) {
                changeMetaState(metaId, CommonTaskStatusEnum.READY);
            }
            throw new ManagerException(e.getMessage());
        }
    }

    public void close(Mapping mapping) {
        Puller puller = getPuller(mapping);

        // 标记停止中
        String metaId = mapping.getMetaId();
        changeMetaState(metaId, CommonTaskStatusEnum.STOPPING);

        puller.close(metaId);
        clusterService.releaseLease(metaId);
    }

    /**
     * 仅停止本进程 Puller，不改集群任务状态。
     *
     * @param mapping 驱动
     */
    public void stopLocal(Mapping mapping) {
        Puller puller = getPuller(mapping);
        String metaId = mapping.getMetaId();
        puller.close(metaId);
        if (clusterService.hasValidLease(metaId)) {
            clusterService.releaseLease(metaId);
        }
    }

    /**
     * 本进程是否已启动该驱动。
     *
     * @param metaId Meta ID
     * @return true 已启动
     */
    public boolean isLocalActive(String metaId) {
        for (Puller puller : map.values()) {
            if (puller.isActive(metaId)) {
                return true;
            }
        }
        return false;
    }

    private void prepareClusterStart(Mapping mapping) {
        String metaId = mapping.getMetaId();
        String model = mapping.getModel();
        Meta meta = metaProfile.getMeta(metaId);
        boolean incrementPhase = isIncrementPhase(meta, model);
        if (clusterService.isLeader() && ModelEnum.isFull(model) && !incrementPhase) {
            clusterService.assignTableGroups(mapping.getId());
        }
        if (!ModelEnum.isIncrement(model) && !incrementPhase) {
            return;
        }
        if (!clusterService.isStandalone() && !clusterService.isLeader()) {
            throw new ManagerException("当前节点未分配该增量任务");
        }
        if (!clusterService.hasValidLease(metaId) && !clusterService.tryAcquireLease(metaId)) {
            throw new ManagerException("当前节点未分配该增量任务");
        }
    }

    private boolean isIncrementPhase(Meta meta, String model) {
        if (meta == null || !StringUtil.equals(ModelEnum.FULLINCREMENT.getCode(), model)) {
            return false;
        }
        String phase = meta.getSnapshot() == null ? null
                : meta.getSnapshot().get(ParserEnum.FULL_INCREMENT_PHASE.getCode());
        return ModelEnum.isIncrement(phase);
    }

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

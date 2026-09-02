package org.dbsyncer.manager;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.event.ClosedEvent;
import org.dbsyncer.manager.impl.ConnectorInstanceBinder;
import org.dbsyncer.parser.MappingRuntimeService;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public class ManagerFactory implements MappingRuntimeService, ApplicationListener<ClosedEvent> {

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private Map<String, Puller> map;

    @Resource
    private ClusterService clusterService;

    @Resource
    private ConnectorInstanceBinder connectorInstanceBinder;

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
        if (puller.isActive(metaId)) {
            return;
        }
        Meta current = metaProfile.getMeta(metaId);
        boolean alreadyRunning = current != null && current.getState() == CommonTaskStatusEnum.RUNNING.getCode();
        boolean runLocal = clusterService.prepareTaskStart(mapping.getId(), mapping.getModel());
        changeMetaState(metaId, CommonTaskStatusEnum.RUNNING);
        if (!runLocal) {
            return;
        }
        connectorInstanceBinder.bind(mapping);
        try {
            puller.start(mapping, autoRecovery);
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
        if (puller.isActive(metaId)) {
            return;
        }
        if (!clusterService.assertTaskWritable(mapping.getId())) {
            return;
        }
        connectorInstanceBinder.bind(mapping);
        puller.start(mapping, autoRecovery);
    }

    public void close(Mapping mapping) {
        Puller puller = getPuller(mapping);

        // 标记停止中
        String metaId = mapping.getMetaId();
        changeMetaState(metaId, CommonTaskStatusEnum.STOPPING);

        puller.close(metaId);
        clusterService.clearTaskSchedule(mapping.getId());
    }

    /**
     * 仅停止本进程 Puller，不改集群任务状态。
     *
     * @param mapping 驱动
     */
    @Override
    public void stopLocal(Mapping mapping) {
        Puller puller = getPuller(mapping);
        String metaId = mapping.getMetaId();
        puller.close(metaId);
    }

    /**
     * 本进程是否已启动该驱动。
     *
     * @param metaId Meta ID
     * @return true 已启动
     */
    @Override
    public boolean isLocalActive(String metaId) {
        for (Puller puller : map.values()) {
            if (puller.isActive(metaId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void changeMetaState(String metaId, CommonTaskStatusEnum status) {
        Meta meta = metaProfile.getMeta(metaId);
        int code = status.getCode();
        if (null != meta && meta.getState() != code) {
            // 只改 STATE，避免整行回写覆盖并发 increment 的 success/fail
            metaProfile.updateMetaState(metaId, code);
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

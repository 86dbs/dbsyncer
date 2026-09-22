/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.Puller;
import org.dbsyncer.manager.event.ClosedEvent;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.service.TaskManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Map;

/**
 * 本机任务执行器：按同步方式路由到对应 Puller。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-08
 */
@Component
public final class TaskManagerImpl implements TaskManager, ApplicationListener<ClosedEvent> {

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private FullIncrementPuller fullIncrementPuller;

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private Map<String, Puller> map;

    @Override
    public void start(ConfigModel configModel, boolean autoRecovery) {
        Mapping mapping = (Mapping) configModel;
        getPuller(mapping).start(mapping, autoRecovery);
    }

    @Override
    public void stop(String taskId) {
        Mapping mapping = requireMapping(taskId);
        getPuller(mapping).close(mapping.getMetaId());
    }

    @Override
    public void prepareFullIncrement(String taskId) {
        Mapping mapping = requireMapping(taskId);
        if (!StringUtil.equals(ModelEnum.FULL_INCREMENT.getCode(), mapping.getModel())) {
            return;
        }
        fullIncrementPuller.prepareFullPhase(mapping);
    }

    @Override
    public void switchToIncrementAfterFull(String taskId) {
        Mapping mapping = requireMapping(taskId);
        if (!StringUtil.equals(ModelEnum.FULL_INCREMENT.getCode(), mapping.getModel())) {
            return;
        }
        fullIncrementPuller.switchToIncrement(mapping);
    }

    @Override
    public void completeBatchFull(String taskId) {
        Mapping mapping = requireMapping(taskId);
        applicationContext.publishEvent(new ClosedEvent(applicationContext, mapping.getMetaId()));
    }

    private Mapping requireMapping(String taskId) {
        Mapping mapping = taskProfile.getMapping(taskId);
        Assert.notNull(mapping, String.format("同步任务不存在: %s", taskId));
        return mapping;
    }

    private Puller getPuller(Mapping mapping) {
        String model = mapping.getModel();
        return map.get(model.concat("Puller"));
    }

    @Override
    public void onApplicationEvent(ClosedEvent event) {
        changeMetaState(event.getMetaId(), CommonTaskStatusEnum.READY);
        // 集群：排空/收口后再回收本机连接（用户停止、自然结束、失败）
        releaseMappingConnectors(event.getMetaId());
    }

    public void changeMetaState(String metaId, CommonTaskStatusEnum status) {
        Meta meta = metaProfile.getMeta(metaId);
        int code = status.getCode();
        if (null != meta && meta.getState() != code) {
            long now = Instant.now().toEpochMilli();
            meta.setState(code);
            meta.setUpdateTime(now);
            // 进入运行中时记录本轮启动时间，供耗时（updateTime - startTime）计算
            if (CommonTaskStatusEnum.RUNNING == status) {
                meta.setStartTime(now);
            }
            metaProfile.updateMeta(meta);
        }
    }

    /**
     * 任务关闭后释放本机源/目标连接（与 PreloadTemplate.reConnect 同一套 instanceId）。
     *
     * @param metaId Meta ID
     */
    private void releaseMappingConnectors(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null || StringUtil.isBlank(meta.getTaskId())) {
            return;
        }
        Mapping mapping = taskProfile.getMapping(meta.getTaskId());
        if (mapping == null) {
            return;
        }
        connectorFactory.disconnect(ConnectorInstanceUtil.buildConnectorInstanceId(
                mapping.getId(), mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX));
        connectorFactory.disconnect(ConnectorInstanceUtil.buildConnectorInstanceId(
                mapping.getId(), mapping.getTargetConnectorId(), ConnectorInstanceUtil.TARGET_SUFFIX));
    }

}

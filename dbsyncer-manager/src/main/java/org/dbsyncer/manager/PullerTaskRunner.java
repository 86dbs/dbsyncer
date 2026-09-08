/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager;

import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.TaskRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 本机任务执行器：按同步方式路由到对应 Puller，并在启动时注册到控制面。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-08
 */
@Component
public final class PullerTaskRunner implements TaskRunner {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private List<Puller> pullers;

    @Resource
    private ClusterService clusterService;

    private final Map<ModelEnum, Puller> pullerMap = new EnumMap<>(ModelEnum.class);

    @PostConstruct
    private void init() {
        for (Puller puller : pullers) {
            ModelEnum model = puller.getModel();
            Assert.notNull(model, String.format("同步方式不能为空: %s", puller.getClass().getName()));
            Puller exist = pullerMap.put(model, puller);
            Assert.isNull(exist, String.format("同步方式重复注册: %s", model.getCode()));
        }
        clusterService.bindTaskRunner(this);
    }

    @Override
    public void start(String taskId, boolean autoRecovery) {
        Mapping mapping = requireMapping(taskId);
        getPuller(mapping).start(mapping, autoRecovery);
    }

    @Override
    public void stop(String taskId) {
        Mapping mapping = requireMapping(taskId);
        getPuller(mapping).close(mapping.getMetaId());
    }

    private Mapping requireMapping(String taskId) {
        Mapping mapping = profileComponent.getMapping(taskId);
        Assert.notNull(mapping, String.format("驱动不存在: %s", taskId));
        return mapping;
    }

    private Puller getPuller(Mapping mapping) {
        String model = mapping.getModel();
        Assert.hasText(model, "同步方式不能为空");
        Assert.hasText(mapping.getMetaId(), "任务ID不能为空");

        Puller puller = pullerMap.get(ModelEnum.getModelEnum(model));
        Assert.notNull(puller, String.format("未知的同步方式: %s", model));
        return puller;
    }
}

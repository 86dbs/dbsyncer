/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.manager.event.ClosedEvent;
import org.dbsyncer.manager.impl.ConnectorInstanceBinder;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Instant;

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
    private ClusterService clusterService;

    @Resource
    private ConnectorInstanceBinder connectorInstanceBinder;

    @Override
    public void onApplicationEvent(ClosedEvent event) {
        changeMetaState(event.getMetaId(), CommonTaskStatusEnum.READY);
        // 集群：排空/收口后再回收本机连接（用户停止、自然结束、失败）
        releaseMappingConnectors(event.getMetaId());
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
        // 标记运行中
        changeMetaState(mapping.getMetaId(), CommonTaskStatusEnum.RUNNING);

        try {
            clusterService.start(mapping.getId(), mapping.getModel(), autoRecovery);
        } catch (Exception e) {
            // rollback
            changeMetaState(mapping.getMetaId(), CommonTaskStatusEnum.READY);
            throw new ManagerException(e.getMessage());
        }
    }

    public void close(Mapping mapping) {
        // 标记停止中
        String metaId = mapping.getMetaId();
        changeMetaState(metaId, CommonTaskStatusEnum.STOPPING);

        clusterService.stop(mapping.getId());
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
            profileComponent.editConfigModel(meta);
        }
    }

    /**
     * 集群下任务关闭后释放本机连接。
     *
     * @param metaId Meta ID
     */
    private void releaseMappingConnectors(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta == null) {
            return;
        }
        Mapping mapping = profileComponent.getMapping(meta.getTaskId());
        connectorInstanceBinder.release(mapping);
    }
}

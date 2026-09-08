package org.dbsyncer.manager;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.manager.event.ClosedEvent;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

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
        Assert.notNull(mapping, "驱动不能为空");
        Assert.hasText(mapping.getId(), "驱动ID不能为空");
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
        Assert.notNull(mapping, "驱动不能为空");
        Assert.hasText(mapping.getId(), "驱动ID不能为空");

        // 标记停止中
        String metaId = mapping.getMetaId();
        changeMetaState(metaId, CommonTaskStatusEnum.STOPPING);

        clusterService.stop(mapping.getId());
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
}

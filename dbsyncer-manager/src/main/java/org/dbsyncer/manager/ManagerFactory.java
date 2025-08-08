package org.dbsyncer.manager;

import org.dbsyncer.manager.event.ClosedEvent;
import org.dbsyncer.manager.impl.FullPuller;
import org.dbsyncer.manager.impl.IncrementPuller;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public class ManagerFactory implements ApplicationListener<ClosedEvent>, ApplicationContextAware {

    @Resource
    private ProfileComponent profileComponent;

    // 为每个mapping存储独立的puller实例
    private final Map<String, Puller> mappingPullerMap = new ConcurrentHashMap<>();
    
    private ConfigurableApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    @Override
    public void onApplicationEvent(ClosedEvent event) {
        changeMetaState(event.getMetaId(), MetaEnum.READY);
    }

    public void start(Mapping mapping) {
        // 为每个mapping添加同步锁，确保线程安全
        synchronized (mapping.getMetaId()) {
            Puller puller = getPuller(mapping);

            // 标记运行中
            changeMetaState(mapping.getMetaId(), MetaEnum.RUNNING);

            try {
                puller.start(mapping);
            } catch (Exception e) {
                // rollback
                changeMetaState(mapping.getMetaId(), MetaEnum.READY);
                throw new ManagerException(e.getMessage());
            }
        }
    }

    public void close(Mapping mapping) {
        String metaId = mapping.getMetaId();
        Puller puller = mappingPullerMap.get(metaId);

        // 标记停止中
        changeMetaState(metaId, MetaEnum.STOPPING);

        if (puller != null) {
            puller.close();
            mappingPullerMap.remove(metaId);
        }
    }

    public void changeMetaState(String metaId, MetaEnum metaEnum) {
        Meta meta = profileComponent.getMeta(metaId);
        int code = metaEnum.getCode();
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

        // 为每个mapping创建独立的puller实例
        return mappingPullerMap.computeIfAbsent(metaId, k -> {
            Puller puller;
            switch (model) {
                case "full":
                    puller = new FullPuller(
                        applicationContext,
                        applicationContext.getBean(org.dbsyncer.parser.ParserComponent.class),
                        applicationContext.getBean(ProfileComponent.class),
                        applicationContext.getBean(org.dbsyncer.parser.LogService.class)
                    );
                    break;
                case "increment":
                    IncrementPuller pullerIns = new IncrementPuller(
                        applicationContext,
                        applicationContext.getBean(org.dbsyncer.parser.flush.impl.BufferActuatorRouter.class),
                        applicationContext.getBean(org.dbsyncer.common.scheduled.ScheduledTaskService.class),
                        applicationContext.getBean(org.dbsyncer.connector.base.ConnectorFactory.class),
                        applicationContext.getBean(ProfileComponent.class),
                        applicationContext.getBean(org.dbsyncer.parser.LogService.class),
                        applicationContext.getBean(org.dbsyncer.parser.TableGroupContext.class)
                    );
                    pullerIns.init();
                    puller = pullerIns;
                    break;
                default:
                    throw new ManagerException(String.format("未知的同步方式: %s", model));
            }
            
            // 将puller注册为Spring事件监听器
            if (applicationContext != null) {
                applicationContext.addApplicationListener((ApplicationListener<?>) puller);
            }
            
            return puller;
        });
    }

}
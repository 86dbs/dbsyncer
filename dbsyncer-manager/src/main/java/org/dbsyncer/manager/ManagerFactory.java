package org.dbsyncer.manager;

import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
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
public class ManagerFactory {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private Map<String, Puller> map;

    public void start(Mapping mapping) {
        Puller puller = getPuller(mapping);

        // 标记运行中，使用Meta类的统一方法
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        meta.saveState(MetaEnum.RUNNING);

        try {
            puller.start(mapping);
        } catch (Exception e) {
            // 记录异常状态和异常信息到Meta对象，使用统一方法
            meta.saveState(MetaEnum.ERROR, e.getMessage());
            throw new ManagerException(e.getMessage());
        }
    }

    public void close(Mapping mapping) {
        Puller puller = getPuller(mapping);

        // 标记停止中，使用Meta类的统一方法
        String metaId = mapping.getMetaId();
        Meta meta = profileComponent.getMeta(metaId);
        meta.saveState(MetaEnum.STOPPING);

        puller.close(meta);
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
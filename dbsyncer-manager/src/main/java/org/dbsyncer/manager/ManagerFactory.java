package org.dbsyncer.manager;

import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
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
public class ManagerFactory {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private Map<String, Puller> map;

    @Resource
    private LogService logService;

    public void start(Mapping mapping) throws Exception {
        Puller puller = getPuller(mapping);

        // 标记运行中，使用Meta类的统一方法
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        meta.setBeginTime(Instant.now().toEpochMilli());
        meta.saveState(MetaEnum.RUNNING);
        
        // 任务启动后禁止编辑
        mapping.setDisableEdit(true);
        mapping.setUpdateTime(Instant.now().toEpochMilli());
        profileComponent.editConfigModel(mapping);
        
        logService.log(LogType.MappingLog.RUNNING, String.format("设置Meta状态为RUNNING: mappingId=%s, metaId=%s", mapping.getId(), mapping.getMetaId()));

        try {
            puller.start(mapping);
            logService.log(LogType.MappingLog.RUNNING, String.format("Puller启动成功: mappingId=%s, metaId=%s", mapping.getId(), mapping.getMetaId()));
        } catch (Exception e) {
            // 记录异常状态和异常信息到Meta对象，使用统一方法
            meta.saveState(MetaEnum.ERROR, e.getMessage());
            logService.log(LogType.MappingLog.RUNNING, String.format("Puller启动失败: mappingId=%s, metaId=%s, error=%s", mapping.getId(), mapping.getMetaId(), e.getMessage()));
            throw new ManagerException(e.getMessage());
        }
    }

    public void close(Mapping mapping) throws Exception {
        Puller puller = getPuller(mapping);
        puller.close(mapping);
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
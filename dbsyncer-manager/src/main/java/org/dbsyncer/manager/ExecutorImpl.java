package org.dbsyncer.manager;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.manager.extractor.Extractor;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-04-26 23:40
 */
@Component
public class ExecutorImpl implements Executor, ApplicationContextAware, ApplicationListener<ClosedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    private Map<String, Extractor> map;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        map = applicationContext.getBeansOfType(Extractor.class);
    }

    @Override
    public void start(Mapping mapping) {
        // 获取数据源连接器
        Connector connector = manager.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "数据源配置不能为空.");

        Extractor extractor = getExtractor(mapping);

        // 标记运行中
        changeMetaState(mapping.getMetaId(), MetaEnum.RUNNING);

        extractor.start(mapping);
    }

    @Override
    public void close(Mapping mapping) {
        Extractor extractor = getExtractor(mapping);

        String metaId = mapping.getMetaId();
        changeMetaState(metaId, MetaEnum.STOPPING);

        extractor.close(metaId);
    }

    @Override
    public void onApplicationEvent(ClosedEvent event) {
        // 异步监听任务关闭事件
        changeMetaState(event.getId(), MetaEnum.READY);
    }

    private Extractor getExtractor(Mapping mapping) {
        Assert.notNull(mapping, "驱动不能为空");
        String model = mapping.getModel();
        String metaId = mapping.getMetaId();
        Assert.hasText(model, "同步方式不能为空");
        Assert.hasText(metaId, "任务ID不能为空");

        Extractor extractor = map.get(model.concat("Extractor"));
        Assert.notNull(extractor, String.format("未知的同步方式: %s", model));
        return extractor;
    }

    private void changeMetaState(String metaId, MetaEnum metaEnum){
        Meta meta = manager.getMeta(metaId);
        meta.setState(metaEnum.getCode());
        manager.editMeta(meta);
    }

}

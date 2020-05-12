package org.dbsyncer.manager.puller.impl;

import org.dbsyncer.common.event.IncrementRefreshEvent;
import org.dbsyncer.common.model.Task;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.manager.puller.AbstractPuller;
import org.dbsyncer.listener.Extractor;
import org.dbsyncer.parser.model.ListenerConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.puller.Increment;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * 增量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class IncrementPuller extends AbstractPuller implements ApplicationContextAware, ApplicationListener<IncrementRefreshEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Listener listener;

    @Autowired
    private Manager manager;

    private Map<String, Extractor> map = new ConcurrentHashMap<>();

    private Map<String, Increment> handle;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        handle = applicationContext.getBeansOfType(Increment.class);
    }

    @Override
    public void asyncStart(Mapping mapping) {
        final String mappingId = mapping.getId();
        final String metaId = mapping.getMetaId();
        try {
            // log/timing
            ListenerConfig listenerConfig = mapping.getListener();
            String listenerType = listenerConfig.getListenerType();
            String type = StringUtil.toLowerCaseFirstOne(listenerType).concat("Increment");
            Increment increment = handle.get(type);
            Assert.notNull(increment, "未知的增量同步方式.");
            Connector connector = manager.getConnector(mapping.getSourceConnectorId());
            Assert.notNull(connector, "连接器不能为空.");
            Extractor extractor = listener.createExtractor(connector.getConfig());
            Assert.notNull(extractor, "未知的连接器配置.");
            map.putIfAbsent(metaId, extractor);

            // 执行任务
            logger.info("启动任务:{}", metaId);
            extractor = map.get(metaId);
            increment.execute(mappingId, metaId, extractor);
        } catch (Exception e) {
            logger.error(e.getMessage());
            map.remove(metaId);
            publishClosedEvent(metaId);
        } finally {
            logger.info("启动成功:{}", metaId);
        }
    }

    @Override
    public void close(String metaId) {
        Extractor extractor = map.get(metaId);
        if (null != extractor) {
            extractor.close();
        }
    }

    @Override
    public void onApplicationEvent(IncrementRefreshEvent event) {
        // 异步监听任务刷新事件
        flush(event.getTask());
    }

    private void flush(Task task) {
        Meta meta = manager.getMeta(task.getId());
        Assert.notNull(meta, "检查meta为空.");
        manager.editMeta(meta);
    }

}
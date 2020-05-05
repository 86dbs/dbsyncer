package org.dbsyncer.manager.extractor.impl;

import org.dbsyncer.common.task.Task;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.extractor.AbstractExtractor;
import org.dbsyncer.manager.extractor.Increment;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
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
public class IncrementExtractor extends AbstractExtractor implements ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Listener listener;

    @Autowired
    private Manager manager;

    private Map<String, Increment> pull;

    private Map<String, Task> map = new ConcurrentHashMap<>();

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        pull = applicationContext.getBeansOfType(Increment.class);
    }

    @Override
    public void asyncStart(Mapping mapping) {
        final String metaId = mapping.getMetaId();
        map.putIfAbsent(metaId, new Task(metaId));

        try {
            ListenerConfig listener = mapping.getListener();
            // log / timing
            String type = listener.getListenerType() + "Increment";
            Increment increment = this.pull.get(type);
            Assert.notNull(increment, "未知的同步方式.");

            Connector connector = manager.getConnector(mapping.getSourceConnectorId());
            Assert.notNull(connector, "连接器不能为空.");

            // 执行任务
            logger.info("启动任务:{}", metaId);
            Task task = map.get(metaId);

        } catch (Exception e) {
            // TODO 记录错误日志
            logger.error(e.getMessage());
        } finally {
            map.remove(metaId);
            publishClosedEvent(metaId);
            logger.info("结束任务:{}", metaId);
        }
    }

    @Override
    public void close(String metaId) {
        Task task = map.get(metaId);
        if (null != task) {
            task.stop();
        }
    }

}
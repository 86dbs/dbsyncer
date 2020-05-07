package org.dbsyncer.manager.extractor.impl;

import org.dbsyncer.common.event.IncrementRefreshEvent;
import org.dbsyncer.common.model.Task;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.enums.TaskEnum;
import org.dbsyncer.manager.extractor.AbstractExtractor;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
public class IncrementExtractor extends AbstractExtractor implements ApplicationListener<IncrementRefreshEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Listener listener;

    @Autowired
    private Manager manager;

    private Map<String, Task> map = new ConcurrentHashMap<>();

    @Override
    public void asyncStart(Mapping mapping) {
        ListenerConfig listenerConfig = mapping.getListener();
        Connector connector = manager.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "连接器不能为空.");
        // log/timing
        String type = listenerConfig.getListenerType();
        Task task = TaskEnum.getIncrementTask(type);
        Assert.notNull(task, "未知的增量同步方式.");

        final String metaId = mapping.getMetaId();
        task.setId(metaId);
        map.putIfAbsent(metaId, task);

        try {
            // 执行任务
            logger.info("启动任务:{}", metaId);
            Task t = map.get(metaId);
            listener.execute(t, listenerConfig, connector.getConfig());

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
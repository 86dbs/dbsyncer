package org.dbsyncer.manager.extractor.impl;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.common.task.Task;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.extractor.AbstractExtractor;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 全量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class FullExtractor extends AbstractExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Parser parser;

    @Autowired
    private Manager manager;

    protected Map<String, Task> map = new ConcurrentHashMap<>();

    @Override
    public void asyncStart(Mapping mapping) {
        String metaId = mapping.getMetaId();
        map.putIfAbsent(metaId, new Task());
        Task task = map.get(metaId);

        // TODO 获取数据源连接器
        Connector connector = manager.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "数据源配置不能为空.");

        try {
            logger.info("启动任务:{}", metaId);
            run(task);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            map.remove(metaId);
            publishClosedEvent(metaId);
        }
    }

    @Override
    public void close(String metaId) {
        Task task = map.get(metaId);
        if (null != task) {
            task.stop();
        }
    }

    protected void run(Task task) throws InterruptedException {
        for (; ; ) {
            if (!task.isRunning()) {
                break;
            }
            logger.info("模拟同步休眠5s");
            TimeUnit.SECONDS.sleep(5);
        }
    }

}
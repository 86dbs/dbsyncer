package org.dbsyncer.manager.extractor;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.common.task.Task;
import org.dbsyncer.manager.Manager;
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

    @Autowired
    private ApplicationContext applicationContext;

    protected Map<String, Task> map = new ConcurrentHashMap<>();

    @Override
    public void asyncStart(Mapping mapping) {
        String metaId = mapping.getMetaId();
        logger.info("启动任务:{}", metaId);
        // TODO 获取数据源连接器
        Connector connector = manager.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "数据源配置不能为空.");

        Task task = new Task();
        task.setState(Task.RUNNING);
        task.setTaskCallBack(() -> publishClosedEvent(metaId));
        map.putIfAbsent(metaId, task);

        run(task);
    }

    @Override
    public void asyncClose(String metaId) {
        Task task = map.get(metaId);
        if (null != task) {
            task.setState(Task.STOP);
            logger.info("关闭中...");
        }
    }

    protected void run(Task task) {
        for(;;){
            if(task.isRunning()){
                try {
                    logger.info("模拟同步休眠5s");
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
                continue;
            }
            task.close();
            break;
        }
    }

    protected void publishClosedEvent(String metaId) {
        applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
        logger.info("结束任务:{}", metaId);
    }

}
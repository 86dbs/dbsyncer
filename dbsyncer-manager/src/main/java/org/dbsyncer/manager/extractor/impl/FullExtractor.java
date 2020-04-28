package org.dbsyncer.manager.extractor.impl;

import org.dbsyncer.common.task.Task;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.extractor.AbstractExtractor;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        final String mappingId = mapping.getId();
        final String sourceConnectorId = mapping.getSourceConnectorId();
        final String metaId = mapping.getMetaId();
        int batchNum = mapping.getBatchNum();
        int threadNum = mapping.getThreadNum();
        map.putIfAbsent(metaId, new Task(metaId, batchNum, threadNum));

        try {
            Connector connector = manager.getConnector(sourceConnectorId);
            Assert.notNull(connector, "数据源连接器不能为空.");
            ConnectorConfig config = connector.getConfig();
            Assert.notNull(config, "连接器配置不能为空.");
            List<TableGroup> list = manager.getTableGroupAll(mappingId);
            Assert.notEmpty(list, "映射关系为空");

            logger.info("启动任务:{}", metaId);
            Task task = map.get(metaId);
            task.setBeginTime(System.currentTimeMillis());
            for (TableGroup t : list) {
                if (!task.isRunning()) {
                    break;
                }
                parser.execute(task, config, t);
            }
            task.setEndTime(System.currentTimeMillis());

            // TODO 同步运行结果


        } catch (Exception e) {
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
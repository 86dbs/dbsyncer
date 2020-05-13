package org.dbsyncer.manager.puller.impl;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.listener.DefaultExtractor;
import org.dbsyncer.listener.Extractor;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.enums.IncrementEnum;
import org.dbsyncer.manager.puller.AbstractPuller;
import org.dbsyncer.manager.puller.Increment;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
public class IncrementPuller extends AbstractPuller {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Parser parser;

    @Autowired
    private Listener listener;

    @Autowired
    private Manager manager;

    private Map<String, DefaultExtractor> map = new ConcurrentHashMap<>();

    @Override
    public void asyncStart(Mapping mapping) {
        final String mappingId = mapping.getId();
        final String metaId = mapping.getMetaId();
        try {
            ListenerConfig listenerConfig = mapping.getListener();
            // log/timing
            Increment increment = IncrementEnum.getIncrement(listenerConfig.getListenerType());
            Assert.notNull(increment, "未知的增量同步方式.");
            Connector connector = manager.getConnector(mapping.getSourceConnectorId());
            Assert.notNull(connector, "连接器不能为空.");
            List<TableGroup> list = manager.getTableGroupAll(mappingId);
            Assert.notEmpty(list, "映射关系不能为空");
            Meta meta = manager.getMeta(metaId);
            Assert.notNull(meta, "Meta不能为空.");
            DefaultExtractor extractor = (DefaultExtractor) listener.createExtractor(connector.getConfig());
            Assert.notNull(extractor, "未知的监听配置.");

            // 监听数据变更事件
            extractor.addListener(new DefaultListener(mapping, list));
            extractor.setMap(meta.getMap());
            map.putIfAbsent(metaId, extractor);

            // 执行任务
            logger.info("启动成功:{}", metaId);
            increment.execute(map.get(metaId));
        } catch (Exception e) {
            logger.error("任务:{} 运行异常:{}", metaId, e.getMessage());
        } finally {
            finished(metaId);
        }
    }

    @Override
    public void close(String metaId) {
        Extractor extractor = map.get(metaId);
        if (null != extractor) {
            extractor.close();
        }
    }

    /**
     * TODO 更新待优化，存在性能问题
     *
     * @param metaId
     */
    private void flush(String metaId) {
        Meta meta = manager.getMeta(metaId);
        DefaultExtractor extractor = map.get(metaId);
        if (null != meta && null != extractor) {
            meta.setMap(extractor.getMap());
            manager.editMeta(meta);
        }
    }

    private void finished(String metaId) {
        map.remove(metaId);
        publishClosedEvent(metaId);
    }

    final class DefaultListener implements Event {

        private Mapping          mapping;
        private List<TableGroup> list;

        public DefaultListener(Mapping mapping, List<TableGroup> list) {
            this.mapping = mapping;
            this.list = list;
        }

        @Override
        public void changedEvent(String event, Map<String, Object> before, Map<String, Object> after) {
            // 处理过程有异常向上抛
            list.forEach(tableGroup -> parser.execute(mapping, tableGroup));
            flush(mapping.getMetaId());
        }

    }

}
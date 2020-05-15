package org.dbsyncer.manager.puller.impl;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.listener.DefaultExtractor;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.puller.AbstractPuller;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
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
            Connector connector = manager.getConnector(mapping.getSourceConnectorId());
            Assert.notNull(connector, "连接器不能为空.");
            List<TableGroup> list = manager.getTableGroupAll(mappingId);
            Assert.notEmpty(list, "映射关系不能为空.");
            Meta meta = manager.getMeta(metaId);
            Assert.notNull(meta, "Meta不能为空.");
            DefaultExtractor extractor = listener.createExtractor(connector.getConfig(), mapping.getListener(), meta.getMap());
            Assert.notNull(extractor, "未知的监听配置.");
            long now = System.currentTimeMillis();
            meta.setBeginTime(now);
            meta.setEndTime(now);
            manager.editMeta(meta);

            // 监听数据变更事件
            extractor.addListener(new DefaultListener(mapping, list));
            map.putIfAbsent(metaId, extractor);

            // 执行任务
            logger.info("启动成功:{}", metaId);
            map.get(metaId).run();
        } catch (Exception e) {
            finished(metaId);
            logger.error("运行异常，结束任务{}:{}", metaId, e.getMessage());
        }
    }

    @Override
    public void close(String metaId) {
        DefaultExtractor extractor = map.get(metaId);
        if (null != extractor) {
            extractor.clearAllListener();
            extractor.close();
            finished(metaId);
            logger.info("关闭成功:{}", metaId);
        }
    }

    private void finished(String metaId) {
        map.remove(metaId);
        publishClosedEvent(metaId);
    }

    final class DefaultListener implements Event {

        private Mapping mapping;
        private List<TableGroup> list;
        private String metaId;

        public DefaultListener(Mapping mapping, List<TableGroup> list) {
            this.mapping = mapping;
            this.list = list;
            this.metaId = mapping.getMetaId();
        }

        @Override
        public void changedEvent(String tableName, String event, List<Object> before, List<Object> after) {
            logger.info("监听数据>tableName:{},event:{},before:{}, after:{}", tableName, event, before, after);
            // 处理过程有异常向上抛
            list.forEach(tableGroup -> parser.execute(mapping, tableGroup));
        }

        @Override
        public void flushEvent() {
            // TODO 更新待优化，存在性能问题
            logger.info("flushEvent");
            DefaultExtractor extractor = map.get(metaId);
            if (null != extractor) {
                Meta meta = manager.getMeta(metaId);
                if (null != meta) {
                    meta.setMap(extractor.getMap());
                    manager.editMeta(meta);
                }
            }
        }

    }

}
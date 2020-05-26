package org.dbsyncer.manager.puller.impl;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.listener.config.ExtractorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.config.TableCommandConfig;
import org.dbsyncer.listener.quartz.ScheduledTaskJob;
import org.dbsyncer.listener.quartz.ScheduledTaskService;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.config.FieldPicker;
import org.dbsyncer.manager.puller.AbstractPuller;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 增量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class IncrementPuller extends AbstractPuller implements ScheduledTaskJob, InitializingBean, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Parser parser;

    @Autowired
    private Listener listener;

    @Autowired
    private Manager manager;

    @Autowired
    private LogService logService;

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    private String key;

    private Map<String, AbstractExtractor> map = new ConcurrentHashMap<>();

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
            ConnectorConfig connectorConfig = connector.getConfig();
            ListenerConfig listenerConfig = mapping.getListener();
            List<TableCommandConfig> tableCommandConfig = list.stream().map(t ->
                    new TableCommandConfig(t.getSourceTable().getName(), t.getCommand())
            ).collect(Collectors.toList());

            AbstractExtractor extractor = listener.getExtractor(new ExtractorConfig(connectorConfig, listenerConfig, meta.getMap(), tableCommandConfig));
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
            map.get(metaId).start();
        } catch (Exception e) {
            close(metaId);
            logger.error("运行异常，结束任务{}:{}", metaId, e.getMessage());
        }
    }

    @Override
    public void close(String metaId) {
        AbstractExtractor extractor = map.get(metaId);
        if (null != extractor) {
            extractor.clearAllListener();
            extractor.close();
            map.remove(metaId);
            publishClosedEvent(metaId);
            logger.info("关闭成功:{}", metaId);
        }
    }

    @Override
    public void run() {
        // 定时同步增量信息
        map.forEach((k, v) -> v.flushEvent());
    }

    @Override
    public void afterPropertiesSet() {
        key = UUIDUtil.getUUID();
        scheduledTaskService.start(key, "*/10 * * * * ?", this);
    }

    @Override
    public void destroy() {
        scheduledTaskService.stop(key);
    }

    final class DefaultListener implements Event {

        private Mapping mapping;
        private String metaId;
        private Map<String, List<FieldPicker>> tablePicker;
        private AtomicBoolean changed = new AtomicBoolean();

        public DefaultListener(Mapping mapping, List<TableGroup> list) {
            this.mapping = mapping;
            this.metaId = mapping.getMetaId();
            this.tablePicker = new LinkedHashMap<>();
            list.forEach(t -> {
                final Table table = t.getSourceTable();
                final String tableName = table.getName();
                tablePicker.putIfAbsent(tableName, new ArrayList<>());
                tablePicker.get(tableName).add(new FieldPicker(t, table.getColumn(), t.getFieldMapping()));
            });
        }

        @Override
        public void changedLogEvent(String tableName, String event, List<Object> before, List<Object> after) {
            logger.info("监听数据=> tableName:{}, event:{}, before:{}, after:{}", tableName, event, before, after);

            // 处理过程有异常向上抛
            List<FieldPicker> pickers = tablePicker.get(tableName);
            if (!CollectionUtils.isEmpty(pickers)) {
                pickers.parallelStream().forEach(p -> {
                    DataEvent data = new DataEvent(event, p.getColumns(before), p.getColumns(after));
                    parser.execute(mapping, p.getTableGroup(), data);
                });
            }

            // 标记有变更记录
            changed.compareAndSet(false, true);
        }

        @Override
        public void changedQuartzEvent(String tableName, String event, Map<String, Object> before, Map<String, Object> after) {
            // 处理过程有异常向上抛
            List<FieldPicker> pickers = tablePicker.get(tableName);
            if (!CollectionUtils.isEmpty(pickers)) {
                pickers.parallelStream().forEach(p -> {
                    DataEvent data = new DataEvent(event, before, after);
                    parser.execute(mapping, p.getTableGroup(), data);
                });
            }

            // 标记有变更记录
            changed.compareAndSet(false, true);
        }

        @Override
        public void flushEvent(Map<String, String> map) {
            // 如果有变更，执行更新
            if (changed.compareAndSet(true, false)) {
                Meta meta = manager.getMeta(metaId);
                if (null != meta) {
                    logger.info("同步增量信息:{}>>{}", metaId, map);
                    meta.setMap(map);
                    manager.editMeta(meta);
                }
            }
        }

        @Override
        public void errorEvent(Exception e) {
            logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
        }

    }

}
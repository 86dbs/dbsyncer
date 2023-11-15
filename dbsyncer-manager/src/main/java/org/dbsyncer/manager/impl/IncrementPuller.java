package org.dbsyncer.manager.impl;

import org.dbsyncer.listener.model.ChangedOffset;
import org.dbsyncer.manager.AbstractPuller;
import org.dbsyncer.parser.event.RefreshOffsetEvent;
import org.dbsyncer.connector.scheduled.ScheduledTaskJob;
import org.dbsyncer.connector.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.Extractor;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerTypeEnum;
import org.dbsyncer.listener.quartz.AbstractQuartzExtractor;
import org.dbsyncer.listener.quartz.TableGroupQuartzCommand;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.consumer.impl.LogConsumer;
import org.dbsyncer.parser.consumer.impl.QuartzConsumer;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 增量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public final class IncrementPuller extends AbstractPuller implements ApplicationListener<RefreshOffsetEvent>, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private Listener listener;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private BufferActuatorRouter bufferActuatorRouter;

    private Map<String, Extractor> map = new ConcurrentHashMap<>();

    @PostConstruct
    private void init() {
        scheduledTaskService.start(3000, this);
    }

    @Override
    public void start(Mapping mapping) {
        final String mappingId = mapping.getId();
        final String metaId = mapping.getMetaId();
        logger.info("开始增量同步：{}, {}", metaId, mapping.getName());
        Connector connector = profileComponent.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "连接器不能为空.");
        List<TableGroup> list = profileComponent.getSortedTableGroupAll(mappingId);
        Assert.notEmpty(list, "映射关系不能为空.");
        Meta meta = profileComponent.getMeta(metaId);
        Assert.notNull(meta, "Meta不能为空.");

        Thread worker = new Thread(() -> {
            try {
                long now = Instant.now().toEpochMilli();
                meta.setBeginTime(now);
                meta.setEndTime(now);
                profileComponent.editConfigModel(meta);
                map.putIfAbsent(metaId, getExtractor(mapping, connector, list, meta));
                map.get(metaId).start();
            } catch (Exception e) {
                close(metaId);
                logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
                logger.error("运行异常，结束增量同步{}:{}", metaId, e.getMessage());
            }
        });
        worker.setName(new StringBuilder("increment-worker-").append(mapping.getId()).toString());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public void close(String metaId) {
        Extractor extractor = map.get(metaId);
        if (null != extractor) {
            bufferActuatorRouter.unbind(metaId);
            extractor.close();
        }
        map.remove(metaId);
        publishClosedEvent(metaId);
        logger.info("关闭成功:{}", metaId);
    }

    @Override
    public void onApplicationEvent(RefreshOffsetEvent event) {
        List<ChangedOffset> offsetList = event.getOffsetList();
        if (!CollectionUtils.isEmpty(offsetList)) {
            offsetList.forEach(offset -> {
                if (offset.isRefreshOffset() && map.containsKey(offset.getMetaId())) {
                    map.get(offset.getMetaId()).refreshEvent(offset);
                }
            });
        }
    }

    @Override
    public void run() {
        // 定时同步增量信息
        map.values().forEach(extractor -> extractor.flushEvent());
    }

    private AbstractExtractor getExtractor(Mapping mapping, Connector connector, List<TableGroup> list, Meta meta) throws InstantiationException, IllegalAccessException {
        ConnectorConfig connectorConfig = connector.getConfig();
        ListenerConfig listenerConfig = mapping.getListener();
        // timing/log
        final String listenerType = listenerConfig.getListenerType();

        AbstractExtractor extractor = null;
        // 默认定时抽取
        if (ListenerTypeEnum.isTiming(listenerType)) {
            AbstractQuartzExtractor quartzExtractor = listener.getExtractor(ListenerTypeEnum.TIMING, connectorConfig.getConnectorType(), AbstractQuartzExtractor.class);
            quartzExtractor.setCommands(list.stream().map(t -> new TableGroupQuartzCommand(t.getSourceTable(), t.getCommand())).collect(Collectors.toList()));
            quartzExtractor.register(new QuartzConsumer().init(bufferActuatorRouter, profileComponent, logService, meta, mapping, list));
            extractor = quartzExtractor;
        }

        // 基于日志抽取
        if (ListenerTypeEnum.isLog(listenerType)) {
            extractor = listener.getExtractor(ListenerTypeEnum.LOG, connectorConfig.getConnectorType(), AbstractExtractor.class);
            extractor.register(new LogConsumer().init(bufferActuatorRouter, profileComponent, logService, meta, mapping, list));
        }

        if (null != extractor) {
            Set<String> filterTable = new HashSet<>();
            List<Table> sourceTable = new ArrayList<>();
            list.forEach(t -> {
                Table table = t.getSourceTable();
                if (!filterTable.contains(t.getName())) {
                    sourceTable.add(table);
                }
                filterTable.add(table.getName());
            });

            extractor.setConnectorFactory(connectorFactory);
            extractor.setScheduledTaskService(scheduledTaskService);
            extractor.setConnectorConfig(connectorConfig);
            extractor.setListenerConfig(listenerConfig);
            extractor.setFilterTable(filterTable);
            extractor.setSourceTable(sourceTable);
            extractor.setSnapshot(meta.getSnapshot());
            extractor.setMetaId(meta.getId());
            return extractor;
        }

        throw new ManagerException("未知的监听配置.");
    }

}
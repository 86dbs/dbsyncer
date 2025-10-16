/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.Puller;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupContext;
import org.dbsyncer.parser.consumer.ParserConsumer;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.enums.SyncPhaseEnum;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.sdk.config.ListenerConfig;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.AbstractListener;
import org.dbsyncer.sdk.listener.AbstractQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.TableGroupQuartzCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 增量同步
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2020-04-26 15:28
 */
@Component
public final class IncrementPuller implements ScheduledTaskJob, Puller {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private BufferActuatorRouter bufferActuatorRouter;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private LogService logService;

    @Resource
    private TableGroupContext tableGroupContext;

    @PostConstruct
    private void init() {
        scheduledTaskService.start(3000, this);
    }

    @Override
    public void start(Mapping mapping) {
        final String mappingId = mapping.getId();
        final String metaId = mapping.getMetaId();
        Connector connector = profileComponent.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "连接器不能为空.");
        Connector targetConnector = profileComponent.getConnector(mapping.getTargetConnectorId());
        Assert.notNull(targetConnector, "目标连接器不能为空.");
        List<TableGroup> list = profileComponent.getSortedTableGroupAll(mappingId);
        Assert.notEmpty(list, "表映射关系不能为空，请先添加源表到目标表关系.");
        Meta meta = profileComponent.getMeta(metaId);
        Assert.notNull(meta, "Meta不能为空.");
        meta.setSyncPhase(SyncPhaseEnum.INCREMENTAL);

        Thread worker = new Thread(() -> {
            try {
                Listener listener = meta.getListener();
                if (listener == null) {
                    logger.info("开始增量同步：{}, {}", metaId, mapping.getName());
                    long now = Instant.now().toEpochMilli();
                    meta.setBeginTime(now);
                    meta.setEndTime(now);
                    profileComponent.editConfigModel(meta);
                    tableGroupContext.put(mapping, list);
                    listener = getListener(mapping, connector, list, meta);
                    meta.setListener(listener);
                }
                listener.start();
            } catch (Exception e) {
                // 记录运行时异常状态和异常信息
                meta.saveState(MetaEnum.ERROR, e.getMessage());
                close(mapping);
                logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
                logger.error("运行异常，结束增量同步：{}", metaId, e);
            }
        });
        worker.setName("increment-worker-" + mapping.getId());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public void close(Mapping mapping) {
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        if (meta != null) {
            Listener listener = meta.getListener();
            if (listener != null) {
                listener.close();
                meta.setListener(null);
            }
            bufferActuatorRouter.unbind(meta.getId());
            tableGroupContext.clear(meta.getId());
            if (!meta.isError()) {
                meta.resetState();
            }
            logger.info("关闭成功:{}", meta.getId());
        }
    }

    @Override
    public void run() {
        profileComponent.getMetaAll().forEach(meta -> {
            Listener listener = meta.getListener();
            if (listener != null) {
                listener.flushEvent();
            }
        });
    }

    private Listener getListener(Mapping mapping, Connector connector, List<TableGroup> list, Meta meta) {
        ConnectorConfig connectorConfig = connector.getConfig();
        ListenerConfig listenerConfig = mapping.getListener();
        String listenerType = listenerConfig.getListenerType();

        Listener listener = connectorFactory.getListener(connectorConfig.getConnectorType(), listenerType);
        if (null == listener) {
            throw new ManagerException(
                    String.format("Unsupported listener type \"%s\".", connectorConfig.getConnectorType()));
        }
        listener.register(new ParserConsumer(bufferActuatorRouter, profileComponent, logService, meta.getId(), list));

        // 默认定时抽取
        if (ListenerTypeEnum.isTiming(listenerType) && listener instanceof AbstractQuartzListener) {
            AbstractQuartzListener quartzListener = (AbstractQuartzListener) listener;
            List<TableGroupQuartzCommand> quartzCommands = list.stream().map(t -> {
                final TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, t);
                final Picker picker = new Picker(group);
                List<Field> fields = picker.getSourceFields();
                Assert.notEmpty(fields, "表字段映射关系不能为空：" + group.getSourceTable().getName() + " > " + group.getTargetTable().getName());
                return new TableGroupQuartzCommand(t.getSourceTable(), fields, t.getTargetTable(), t.getCommand(), group.getPlugin(), group.getPluginExtInfo());
            }).collect(Collectors.toList());
            quartzListener.setCommands(quartzCommands);
        }

        if (listener instanceof AbstractListener) {
            AbstractListener abstractListener = (AbstractListener) listener;
            Set<String> filterTable = new HashSet<>();
            List<Table> sourceTable = new ArrayList<>();
            list.forEach(t -> {
                Table table = t.getSourceTable();
                if (!filterTable.contains(t.getName())) {
                    sourceTable.add(table);
                }
                filterTable.add(table.getName());
            });

            abstractListener
                    .setConnectorService(connectorFactory.getConnectorService(connectorConfig.getConnectorType()));
            abstractListener.setConnectorInstance(connectorFactory.connect(connectorConfig));
            abstractListener.setTargetConnectorInstance(connectorFactory.connect(targetConnectorConfig));
            abstractListener.setScheduledTaskService(scheduledTaskService);
            abstractListener.setConnectorConfig(connectorConfig);
            abstractListener.setListenerConfig(listenerConfig);
            abstractListener.setFilterTable(filterTable);
            abstractListener.setSourceTable(sourceTable);
            abstractListener.setSnapshot(meta.getSnapshot());
            abstractListener.setMetaId(meta.getId());
        }

        listener.init();
        return listener;
    }
}
package org.dbsyncer.manager.puller;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.Extractor;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerTypeEnum;
import org.dbsyncer.listener.quartz.AbstractQuartzExtractor;
import org.dbsyncer.listener.quartz.TableGroupCommand;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.model.FieldPicker;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * ????????????
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class IncrementPuller extends AbstractPuller implements ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private Parser parser;

    @Resource
    private Listener listener;

    @Resource
    private Manager manager;

    @Resource
    private LogService logService;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Qualifier("taskExecutor")
    @Resource
    private Executor taskExecutor;

    private Map<String, Extractor> map = new ConcurrentHashMap<>();

    @PostConstruct
    private void init() {
        scheduledTaskService.start(3000, this);
    }

    @Override
    public void start(Mapping mapping) {
        final String mappingId = mapping.getId();
        final String metaId = mapping.getMetaId();
        logger.info("?????????????????????{}, {}", metaId, mapping.getName());
        Connector connector = manager.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "?????????????????????.");
        List<TableGroup> list = manager.getSortedTableGroupAll(mappingId);
        Assert.notEmpty(list, "????????????????????????.");
        Meta meta = manager.getMeta(metaId);
        Assert.notNull(meta, "Meta????????????.");

        Thread worker = new Thread(() -> {
            try {
                long now = Instant.now().toEpochMilli();
                meta.setBeginTime(now);
                meta.setEndTime(now);
                manager.editConfigModel(meta);
                map.putIfAbsent(metaId, getExtractor(mapping, connector, list, meta));
                map.get(metaId).start();
            } catch (Exception e) {
                close(metaId);
                logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
                logger.error("?????????????????????????????????{}:{}", metaId, e.getMessage());
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
            extractor.clearAllListener();
            extractor.close();
        }
        map.remove(metaId);
        publishClosedEvent(metaId);
        logger.info("????????????:{}", metaId);
    }

    @Override
    public void run() {
        // ????????????????????????
        map.forEach((k, v) -> v.flushEvent());
    }

    private AbstractExtractor getExtractor(Mapping mapping, Connector connector, List<TableGroup> list, Meta meta)
            throws InstantiationException, IllegalAccessException {
        AbstractConnectorConfig connectorConfig = connector.getConfig();
        ListenerConfig listenerConfig = mapping.getListener();

        // timing/log
        final String listenerType = listenerConfig.getListenerType();

        // ??????????????????
        if (ListenerTypeEnum.isTiming(listenerType)) {
            AbstractQuartzExtractor extractor = listener.getExtractor(ListenerTypeEnum.TIMING, connectorConfig.getConnectorType(), AbstractQuartzExtractor.class);
            extractor.setCommands(list.stream().map(t -> {
                String pk = PrimaryKeyUtil.findOriginalTablePrimaryKey(t.getSourceTable());
                return new TableGroupCommand(pk, t.getCommand());
            }).collect(Collectors.toList()));
            setExtractorConfig(extractor, connectorConfig, listenerConfig, meta.getSnapshot(), new QuartzListener(mapping, list));
            return extractor;
        }

        // ??????????????????
        if (ListenerTypeEnum.isLog(listenerType)) {
            AbstractExtractor extractor = listener.getExtractor(ListenerTypeEnum.LOG, connectorConfig.getConnectorType(), AbstractExtractor.class);
            Set<String> filterTable = new HashSet<>();
            LogListener logListener = new LogListener(mapping, list, extractor);
            logListener.getTablePicker().forEach((k, fieldPickers) -> filterTable.add(k));
            extractor.setFilterTable(filterTable);
            setExtractorConfig(extractor, connectorConfig, listenerConfig, meta.getSnapshot(), logListener);
            return extractor;
        }

        throw new ManagerException("?????????????????????.");
    }

    private void setExtractorConfig(AbstractExtractor extractor, AbstractConnectorConfig connector, ListenerConfig listener,
                                    Map<String, String> snapshot, AbstractListener event) {
        extractor.setConnectorFactory(connectorFactory);
        extractor.setScheduledTaskService(scheduledTaskService);
        extractor.setConnectorConfig(connector);
        extractor.setListenerConfig(listener);
        extractor.setSnapshot(snapshot);
        extractor.addListener(event);
        extractor.setMetaId(event.metaId);
    }

    abstract class AbstractListener implements Event {
        private static final int FLUSH_DELAYED_SECONDS = 30;
        protected Mapping mapping;
        protected String metaId;

        @Override
        public void flushEvent(Map<String, String> snapshot) {
            // 30s????????????????????????
            Meta meta = manager.getMeta(metaId);
            LocalDateTime lastSeconds = LocalDateTime.now().minusSeconds(FLUSH_DELAYED_SECONDS);
            if (meta.getUpdateTime() > Timestamp.valueOf(lastSeconds).getTime()) {
                if (!CollectionUtils.isEmpty(snapshot)) {
                    logger.debug("{}", snapshot);
                }
                forceFlushEvent(snapshot);
            }
        }

        @Override
        public void forceFlushEvent(Map<String, String> snapshot) {
            Meta meta = manager.getMeta(metaId);
            if (null != meta) {
                meta.setSnapshot(snapshot);
                manager.editConfigModel(meta);
            }
        }

        @Override
        public void errorEvent(Exception e) {
            logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
        }

        @Override
        public void interruptException(Exception e) {
            errorEvent(e);
            close(metaId);
        }
    }

    /**
     * </p>????????????
     * <ol>
     * <li>????????????????????????</li>
     * </ol>
     * </p>???????????????
     * </p>???????????? >> ????????????
     * <ul>
     * <li>A >> B</li>
     * <li>A >> C</li>
     * <li>E >> F</li>
     * </ul>
     * </p>PS???
     * <ol>
     * <li>????????????????????????A >> B ?????? A >> C ...</li>
     * </ol>
     */
    final class QuartzListener extends AbstractListener {

        private List<FieldPicker> tablePicker;

        public QuartzListener(Mapping mapping, List<TableGroup> list) {
            this.mapping = mapping;
            this.metaId = mapping.getMetaId();
            this.tablePicker = new LinkedList<>();
            list.forEach(t -> tablePicker.add(new FieldPicker(PickerUtil.mergeTableGroupConfig(mapping, t))));
        }

        @Override
        public void changedEvent(RowChangedEvent rowChangedEvent) {
            final FieldPicker picker = tablePicker.get(rowChangedEvent.getTableGroupIndex());
            TableGroup tableGroup = picker.getTableGroup();
            rowChangedEvent.setSourceTableName(tableGroup.getSourceTable().getName());

            // ??????????????????????????????
            parser.execute(mapping, tableGroup, rowChangedEvent);
        }
    }

    /**
     * </p>????????????
     * <ol>
     * <li>?????????????????????</li>
     * <li>????????????????????????</li>
     * </ol>
     * </p>???????????????
     * </p>???????????? >> ????????????
     * <ul>
     * <li>A >> B</li>
     * <li>A >> C</li>
     * <li>E >> F</li>
     * </ul>
     * </p>PS???
     * <ol>
     * <li>?????????????????????????????????????????????, ?????????????????????????????????????????????.</li>
     * <li>??????A >> B???A >> C?????????A???????????????, A????????????????????????????????????B???C.</li>
     * <li>???????????????????????????????????????.</li>
     * </ol>
     */
    final class LogListener extends AbstractListener {

        private Extractor extractor;
        private Map<String, List<FieldPicker>> tablePicker;
        private AtomicInteger eventCounter;
        private static final int MAX_LOG_CACHE_SIZE = 128;

        public LogListener(Mapping mapping, List<TableGroup> list, Extractor extractor) {
            this.mapping = mapping;
            this.metaId = mapping.getMetaId();
            this.extractor = extractor;
            this.tablePicker = new LinkedHashMap<>();
            this.eventCounter = new AtomicInteger();
            list.forEach(t -> {
                final Table table = t.getSourceTable();
                final String tableName = table.getName();
                List<Field> pkList = t.getTargetTable().getColumn().stream().filter(field -> field.isPk()).collect(Collectors.toList());
                tablePicker.putIfAbsent(tableName, new ArrayList<>());
                TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, t);
                tablePicker.get(tableName).add(new FieldPicker(group, pkList, group.getFilter(), table.getColumn(), group.getFieldMapping()));
            });
        }

        @Override
        public void changedEvent(RowChangedEvent rowChangedEvent) {
            // ??????????????????????????????
            List<FieldPicker> pickers = tablePicker.get(rowChangedEvent.getSourceTableName());
            if (!CollectionUtils.isEmpty(pickers)) {
                pickers.forEach(picker -> {
                    final Map<String, Object> dataMap = picker.getColumns(rowChangedEvent.getDataList());
                    if (picker.filter(dataMap)) {
                        rowChangedEvent.setDataMap(dataMap);
                        parser.execute(mapping, picker.getTableGroup(), rowChangedEvent);
                    }
                });
                eventCounter.set(0);
                return;
            }

            // ??????????????????????????????????????????????????????????????????
            if (eventCounter.incrementAndGet() >= MAX_LOG_CACHE_SIZE) {
                extractor.forceFlushEvent();
                eventCounter.set(0);
            }
        }

        public Map<String, List<FieldPicker>> getTablePicker() {
            return tablePicker;
        }
    }

}
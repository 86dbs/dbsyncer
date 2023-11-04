package org.dbsyncer.manager.puller;

import org.dbsyncer.common.event.ChangedEvent;
import org.dbsyncer.common.event.ChangedOffset;
import org.dbsyncer.common.event.CommonChangedEvent;
import org.dbsyncer.common.event.DDLChangedEvent;
import org.dbsyncer.common.event.RefreshOffsetEvent;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.event.ScanChangedEvent;
import org.dbsyncer.common.event.Watcher;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
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
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.model.FieldPicker;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;
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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
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
    private Manager manager;

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
        Connector connector = manager.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "连接器不能为空.");
        List<TableGroup> list = manager.getSortedTableGroupAll(mappingId);
        Assert.notEmpty(list, "映射关系不能为空.");
        Meta meta = manager.getMeta(metaId);
        Assert.notNull(meta, "Meta不能为空.");

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
        AbstractConnectorConfig connectorConfig = connector.getConfig();
        ListenerConfig listenerConfig = mapping.getListener();
        // timing/log
        final String listenerType = listenerConfig.getListenerType();

        AbstractExtractor extractor = null;
        // 默认定时抽取
        if (ListenerTypeEnum.isTiming(listenerType)) {
            AbstractQuartzExtractor quartzExtractor = listener.getExtractor(ListenerTypeEnum.TIMING, connectorConfig.getConnectorType(), AbstractQuartzExtractor.class);
            quartzExtractor.setCommands(list.stream().map(t -> new TableGroupQuartzCommand(t.getSourceTable(), t.getCommand())).collect(Collectors.toList()));
            quartzExtractor.register(new QuartzConsumer(meta, mapping, list));
            extractor = quartzExtractor;
        }

        // 基于日志抽取
        if (ListenerTypeEnum.isLog(listenerType)) {
            extractor = listener.getExtractor(ListenerTypeEnum.LOG, connectorConfig.getConnectorType(), AbstractExtractor.class);
            extractor.register(new LogConsumer(meta, mapping, list));
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

    abstract class AbstractConsumer<E extends ChangedEvent> implements Watcher {
        protected Meta meta;

        //判断上次是否为ddl，是ddl需要强制刷新下picker
        private boolean ddl;

        public abstract void onChange(E e);

        public void onDDLChanged(DDLChangedEvent event) {
        }

        @Override
        public void changeEvent(ChangedEvent event) {
            event.getChangedOffset().setMetaId(meta.getId());
            if (event instanceof DDLChangedEvent) {
                onDDLChanged((DDLChangedEvent) event);
                return;
            }
            onChange((E) event);
        }

        @Override
        public void flushEvent(Map<String, String> snapshot) {
            meta.setSnapshot(snapshot);
            manager.editConfigModel(meta);
        }

        @Override
        public void errorEvent(Exception e) {
            logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
        }

        @Override
        public long getMetaUpdateTime() {
            return meta.getUpdateTime();
        }

        protected void bind(String tableGroupId) {
            bufferActuatorRouter.bind(meta.getId(), tableGroupId);
        }

        protected void execute(String tableGroupId, ChangedEvent event) {
            bufferActuatorRouter.execute(meta.getId(), tableGroupId, event);
        }

        public boolean isDdl() {
            return ddl;
        }

        public void setDdl(boolean ddl) {
            this.ddl = ddl;
        }
    }

    final class QuartzConsumer extends AbstractConsumer<ScanChangedEvent> {
        private List<FieldPicker> tablePicker = new LinkedList<>();

        public QuartzConsumer(Meta meta, Mapping mapping, List<TableGroup> tableGroups) {
            this.meta = meta;
            tableGroups.forEach(t -> {
                tablePicker.add(new FieldPicker(PickerUtil.mergeTableGroupConfig(mapping, t)));
                bind(t.getId());
            });
        }

        @Override
        public void onChange(ScanChangedEvent event) {
            final FieldPicker picker = tablePicker.get(event.getTableGroupIndex());
            TableGroup tableGroup = picker.getTableGroup();
            event.setSourceTableName(tableGroup.getSourceTable().getName());

            // 定时暂不支持触发刷新增量点事件
            execute(tableGroup.getId(), event);
        }
    }

    final class LogConsumer extends AbstractConsumer<RowChangedEvent> {
        private Mapping mapping;

        private  List<TableGroup> tableGroups;
        private Map<String, List<FieldPicker>> tablePicker = new LinkedHashMap<>();

        public LogConsumer(Meta meta, Mapping mapping, List<TableGroup> tableGroups) {
            this.tableGroups = tableGroups;
            this.meta = meta;
            this.mapping = mapping;
            tableGroups.forEach(t -> {
                final Table table = t.getSourceTable();
                final String tableName = table.getName();
                tablePicker.putIfAbsent(tableName, new ArrayList<>());
                TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, t);
                tablePicker.get(tableName).add(new FieldPicker(group, group.getFilter(), table.getColumn(), group.getFieldMapping()));
                bind(group.getId());
            });
        }

        @Override
        public void onChange(RowChangedEvent event) {
            if (isDdl()){//需要强制刷新 fix https://gitee.com/ghi/dbsyncer/issues/I8DJUR
                this.tablePicker.clear();
                this.tableGroups.forEach(t -> {
                    final Table table = t.getSourceTable();
                    final String tableName = table.getName();
                    tablePicker.putIfAbsent(tableName, new ArrayList<>());
                    TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, t);
                    tablePicker.get(tableName).add(new FieldPicker(group, group.getFilter(), table.getColumn(), group.getFieldMapping()));
                    bind(group.getId());
                });
                setDdl(false);
            }
            process(event, picker -> {
                final Map<String, Object> changedRow = picker.getColumns(event.getDataList());
                if (picker.filter(changedRow)) {
                    event.setChangedRow(changedRow);
                    execute(picker.getTableGroup().getId(), event);
                }
            });
        }

        @Override
        public void onDDLChanged(DDLChangedEvent event) {
            setDdl(true);
            process(event, picker -> execute(picker.getTableGroup().getId(), event));
        }

        private void process(CommonChangedEvent event, Consumer<FieldPicker> consumer) {
            // 处理过程有异常向上抛
            List<FieldPicker> pickers = tablePicker.get(event.getSourceTableName());
            if (!CollectionUtils.isEmpty(pickers)) {
                // 触发刷新增量点事件
                event.getChangedOffset().setRefreshOffset(true);
                pickers.forEach(picker -> consumer.accept(picker));
            }
        }

    }

}
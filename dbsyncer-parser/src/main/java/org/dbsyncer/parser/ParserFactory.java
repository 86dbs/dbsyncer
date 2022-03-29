package org.dbsyncer.parser;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.event.FullRefreshEvent;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.flush.FlushStrategy;
import org.dbsyncer.parser.flush.model.WriterRequest;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/29 22:38
 */
@Component
public class ParserFactory implements Parser {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConnectorFactory connectorFactory;

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private LogService logService;

    @Autowired
    private FlushStrategy flushStrategy;

    @Autowired
    @Qualifier("taskExecutor")
    private Executor taskExecutor;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private BufferActuator writerBufferActuator;

    @Override
    public ConnectorMapper connect(ConnectorConfig config) {
        return connectorFactory.connect(config);
    }

    @Override
    public boolean refreshConnectorConfig(ConnectorConfig config) {
        return connectorFactory.refresh(config);
    }

    @Override
    public boolean isAliveConnectorConfig(ConnectorConfig config) {
        boolean alive = false;
        try {
            alive = connectorFactory.isAlive(config);
        } catch (Exception e) {
            LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
            logService.log(logType, "%s%s", logType.getName(), e.getMessage());
        }
        // 断线重连
        if (!alive) {
            try {
                alive = connectorFactory.refresh(config);
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
            if (alive) {
                logger.info(LogType.ConnectorLog.RECONNECT_SUCCESS.getMessage());
            }
        }
        return alive;
    }

    @Override
    public List<Table> getTable(ConnectorMapper config) {
        return connectorFactory.getTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        Connector connector = getConnector(connectorId);
        ConnectorMapper connectorMapper = connectorFactory.connect(connector.getConfig());
        MetaInfo metaInfo = connectorFactory.getMetaInfo(connectorMapper, tableName);
        if (!CollectionUtils.isEmpty(connector.getTable())) {
            for (Table t : connector.getTable()) {
                if (t.getName().equals(tableName)) {
                    metaInfo.setTableType(t.getType());
                    break;
                }
            }
        }
        return metaInfo;
    }

    @Override
    public Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup) {
        ConnectorConfig connectorConfig = getConnectorConfig(mapping.getSourceConnectorId());
        String sType = connectorConfig.getConnectorType();
        String tType = getConnectorConfig(mapping.getTargetConnectorId()).getConnectorType();
        Table sourceTable = tableGroup.getSourceTable();
        Table targetTable = tableGroup.getTargetTable();
        Table sTable = new Table(sourceTable.getName(), sourceTable.getType(), new ArrayList<>());
        Table tTable = new Table(targetTable.getName(), targetTable.getType(), new ArrayList<>());
        List<FieldMapping> fieldMapping = tableGroup.getFieldMapping();
        if (!CollectionUtils.isEmpty(fieldMapping)) {
            fieldMapping.forEach(m -> {
                if (null != m.getSource()) {
                    sTable.getColumn().add(m.getSource());
                }
                if (null != m.getTarget()) {
                    tTable.getColumn().add(m.getTarget());
                }
            });
        }
        final CommandConfig sourceConfig = new CommandConfig(sType, sTable, sourceTable, tableGroup.getFilter(), connectorConfig);
        final CommandConfig targetConfig = new CommandConfig(tType, tTable, targetTable);
        // 获取连接器同步参数
        Map<String, String> command = connectorFactory.getCommand(sourceConfig, targetConfig);
        return command;
    }

    @Override
    public long getCount(String connectorId, Map<String, String> command) {
        ConnectorMapper connectorMapper = connectorFactory.connect(getConnectorConfig(connectorId));
        return connectorFactory.getCount(connectorMapper, command);
    }

    @Override
    public Connector parseConnector(String json) {
        try {
            JSONObject conn = new JSONObject(json);
            JSONObject config = (JSONObject) conn.remove("config");
            Connector connector = JsonUtil.jsonToObj(conn.toString(), Connector.class);
            Assert.notNull(connector, "Connector can not be null.");
            String connectorType = config.getString("connectorType");
            Class<?> configClass = ConnectorEnum.getConfigClass(connectorType);
            ConnectorConfig obj = (ConnectorConfig) JsonUtil.jsonToObj(config.toString(), configClass);
            connector.setConfig(obj);

            return connector;
        } catch (JSONException e) {
            logger.error(e.getMessage());
            throw new ParserException(e.getMessage());
        }
    }

    @Override
    public <T> T parseObject(String json, Class<T> clazz) {
        T t = JsonUtil.jsonToObj(json, clazz);
        return t;
    }

    @Override
    public List<ConnectorEnum> getConnectorEnumAll() {
        return Arrays.asList(ConnectorEnum.values());
    }

    @Override
    public List<OperationEnum> getOperationEnumAll() {
        return Arrays.asList(OperationEnum.values());
    }

    @Override
    public List<QuartzFilterEnum> getQuartzFilterEnumAll() {
        return Arrays.asList(QuartzFilterEnum.values());
    }

    @Override
    public List<FilterEnum> getFilterEnumAll() {
        return Arrays.asList(FilterEnum.values());
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return Arrays.asList(ConvertEnum.values());
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return Arrays.asList(StorageDataStatusEnum.values());
    }

    @Override
    public void execute(Task task, Mapping mapping, TableGroup tableGroup) {
        final String metaId = task.getId();
        final String sourceConnectorId = mapping.getSourceConnectorId();
        final String targetConnectorId = mapping.getTargetConnectorId();

        ConnectorConfig sConfig = getConnectorConfig(sourceConnectorId);
        Assert.notNull(sConfig, "数据源配置不能为空.");
        ConnectorConfig tConfig = getConnectorConfig(targetConnectorId);
        Assert.notNull(tConfig, "目标源配置不能为空.");
        TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
        Map<String, String> command = group.getCommand();
        Assert.notEmpty(command, "执行命令不能为空.");
        List<FieldMapping> fieldMapping = group.getFieldMapping();
        String sTableName = group.getSourceTable().getName();
        String tTableName = group.getTargetTable().getName();
        Assert.notEmpty(fieldMapping, String.format("数据源表[%s]同步到目标源表[%s], 映射关系不能为空.", sTableName, tTableName));
        // 获取同步字段
        Picker picker = new Picker(fieldMapping);

        // 检查分页参数
        Map<String, String> params = getMeta(metaId).getMap();
        params.putIfAbsent(ParserEnum.PAGE_INDEX.getCode(), ParserEnum.PAGE_INDEX.getDefaultValue());
        int pageSize = mapping.getReadNum();
        int batchSize = mapping.getBatchNum();
        ConnectorMapper sConnectorMapper = connectorFactory.connect(sConfig);
        ConnectorMapper tConnectorMapper = connectorFactory.connect(tConfig);

        for (; ; ) {
            if (!task.isRunning()) {
                logger.warn("任务被中止:{}", metaId);
                break;
            }

            // 1、获取数据源数据
            int pageIndex = Integer.parseInt(params.get(ParserEnum.PAGE_INDEX.getCode()));
            Result reader = connectorFactory.reader(sConnectorMapper, new ReaderConfig(command, new ArrayList<>(), pageIndex, pageSize));
            List<Map> data = reader.getData();
            if (CollectionUtils.isEmpty(data)) {
                params.clear();
                logger.info("完成全量同步任务:{}, [{}] >> [{}]", metaId, sTableName, tTableName);
                break;
            }

            // 2、映射字段
            List<Map> target = picker.pickData(reader.getData());

            // 3、参数转换
            ConvertUtil.convert(group.getConvert(), target);

            // 4、插件转换
            pluginFactory.convert(group.getPlugin(), data, target);

            // 5、写入目标源
            Result writer = writeBatch(tConnectorMapper, command, ConnectorConstant.OPERTION_INSERT, picker.getTargetFields(), target, batchSize);

            // 6、更新结果
            flush(task, writer, target);

            // 7、更新分页数
            params.put(ParserEnum.PAGE_INDEX.getCode(), String.valueOf(++pageIndex));
        }
    }

    @Override
    public void execute(Mapping mapping, TableGroup tableGroup, RowChangedEvent rowChangedEvent) {
        logger.info("Table[{}] {}, before:{}, after:{}", rowChangedEvent.getTableName(), rowChangedEvent.getEvent(),
                rowChangedEvent.getBefore(), rowChangedEvent.getAfter());
        final String metaId = mapping.getMetaId();

        ConnectorMapper tConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId()));
        // 1、获取映射字段
        final String event = rowChangedEvent.getEvent();
        Map<String, Object> data = StringUtil.equals(ConnectorConstant.OPERTION_DELETE, event) ? rowChangedEvent.getBefore() : rowChangedEvent.getAfter();
        Picker picker = new Picker(tableGroup.getFieldMapping(), data);
        Map target = picker.getTargetMap();

        // 2、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), target);

        // 3、插件转换
        pluginFactory.convert(tableGroup.getPlugin(), event, data, target);

        // 4、写入缓冲执行器
        writerBufferActuator.offer(new WriterRequest(metaId, tableGroup.getId(), event, tConnectorMapper, picker.getTargetFields(), tableGroup.getCommand(), target));
    }

    /**
     * 批量写入
     *
     * @param connectorMapper
     * @param command
     * @param fields
     * @param dataList
     * @param batchSize
     * @return
     */
    @Override
    public Result writeBatch(ConnectorMapper connectorMapper, Map<String, String> command, String event, List<Field> fields, List<Map> dataList, int batchSize) {
        // 总数
        int total = dataList.size();
        // 单次任务
        if (total <= batchSize) {
            return connectorFactory.writer(connectorMapper, new WriterBatchConfig(event, command, fields, dataList));
        }

        // 批量任务, 拆分
        int taskSize = total % batchSize == 0 ? total / batchSize : total / batchSize + 1;

        final Result result = new Result();
        final CountDownLatch latch = new CountDownLatch(taskSize);
        int fromIndex = 0;
        int toIndex = batchSize;
        for (int i = 0; i < taskSize; i++) {
            final List<Map> data;
            if (toIndex > total) {
                toIndex = fromIndex + (total % batchSize);
                data = dataList.subList(fromIndex, toIndex);
            } else {
                data = dataList.subList(fromIndex, toIndex);
                fromIndex += batchSize;
                toIndex += batchSize;
            }

            taskExecutor.execute(() -> {
                try {
                    Result w = connectorFactory.writer(connectorMapper, new WriterBatchConfig(event, command, fields, data));
                    // CAS
                    result.getFailData().addAll(w.getFailData());
                    result.getFail().getAndAdd(w.getFail().get());
                    result.getError().append(w.getError());
                } catch (Exception e) {
                    result.getError().append(e.getMessage()).append(System.lineSeparator());
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        return result;
    }

    /**
     * 更新缓存
     *
     * @param task
     * @param writer
     * @param data
     */
    private void flush(Task task, Result writer, List<Map> data) {
        flushStrategy.flushFullData(task.getId(), writer, ConnectorConstant.OPERTION_INSERT, data);

        // 发布刷新事件给FullExtractor
        task.setEndTime(Instant.now().toEpochMilli());
        applicationContext.publishEvent(new FullRefreshEvent(applicationContext, task));
    }

    /**
     * 获取Meta(注: 没有bean拷贝, 便于直接更新缓存)
     *
     * @param metaId
     * @return
     */
    private Meta getMeta(String metaId) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        Assert.notNull(meta, "Meta can not be null.");
        return meta;
    }

    /**
     * 获取连接器
     *
     * @param connectorId
     * @return
     */
    private Connector getConnector(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = cacheService.get(connectorId, Connector.class);
        Assert.notNull(conn, "Connector can not be null.");
        return conn;
    }

    /**
     * 获取连接配置
     *
     * @param connectorId
     * @return
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        return getConnector(connectorId).getConfig();
    }

}
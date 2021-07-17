package org.dbsyncer.parser;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.event.FullRefreshEvent;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.model.Task;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
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
import org.dbsyncer.parser.flush.FlushService;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

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
    private FlushService flushService;

    @Autowired
    private ApplicationContext applicationContext;

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
        return connectorFactory.isAlive(config);
    }

    @Override
    public List<String> getTable(ConnectorMapper config) {
        return connectorFactory.getTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        ConnectorMapper connectorMapper = connectorFactory.connect(getConnectorConfig(connectorId));
        return connectorFactory.getMetaInfo(connectorMapper, tableName);
    }

    @Override
    public Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup) {
        String sType = getConnectorConfig(mapping.getSourceConnectorId()).getConnectorType();
        String tType = getConnectorConfig(mapping.getTargetConnectorId()).getConnectorType();
        Table sourceTable = tableGroup.getSourceTable();
        Table targetTable = tableGroup.getTargetTable();
        Table sTable = new Table().setName(sourceTable.getName()).setColumn(new ArrayList<>());
        Table tTable = new Table().setName(targetTable.getName()).setColumn(new ArrayList<>());
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
        final CommandConfig sourceConfig = new CommandConfig(sType, sTable, sourceTable, tableGroup.getFilter());
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
        try {
            JSONObject obj = new JSONObject(json);
            T t = JsonUtil.jsonToObj(obj.toString(), clazz);
            String format = String.format("%s can not be null.", clazz.getSimpleName());
            Assert.notNull(t, format);
            return t;
        } catch (JSONException e) {
            logger.error(e.getMessage());
            throw new ParserException(e.getMessage());
        }
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
        int threadSize = mapping.getThreadNum();
        int batchSize = mapping.getBatchNum();
        ConnectorMapper sConnectionMapper = connectorFactory.connect(sConfig);
        ConnectorMapper tConnectionMapper = connectorFactory.connect(tConfig);

        for (; ; ) {
            if (!task.isRunning()) {
                logger.warn("任务被中止:{}", metaId);
                break;
            }

            // 1、获取数据源数据
            int pageIndex = Integer.parseInt(params.get(ParserEnum.PAGE_INDEX.getCode()));
            Result reader = connectorFactory.reader(new ReaderConfig(sConnectionMapper, command, new ArrayList<>(), pageIndex, pageSize));
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
            Result writer = writeBatch(tConnectionMapper, command, picker.getTargetFields(), target, threadSize, batchSize);

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
        Map<String, Object> data = StringUtils.equals(ConnectorConstant.OPERTION_DELETE, event) ? rowChangedEvent.getBefore() : rowChangedEvent.getAfter();
        Picker picker = new Picker(tableGroup.getFieldMapping(), data);
        Map target = picker.getTargetMap();

        // 2、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), target);

        // 3、插件转换
        pluginFactory.convert(tableGroup.getPlugin(), event, data, target);

        // 4、写入目标源
        Result writer = connectorFactory.writer(new WriterSingleConfig(tConnectorMapper, picker.getTargetFields(), tableGroup.getCommand(), event, target, rowChangedEvent.getTableName()));

        // 5、更新结果
        flush(metaId, writer, event, picker.getTargetMapList());
    }

    /**
     * 更新缓存
     *
     * @param task
     * @param writer
     * @param data
     */
    private void flush(Task task, Result writer, List<Map> data) {
        flush(task.getId(), writer, ConnectorConstant.OPERTION_INSERT, data);

        // 发布刷新事件给FullExtractor
        task.setEndTime(Instant.now().toEpochMilli());
        applicationContext.publishEvent(new FullRefreshEvent(applicationContext, task));
    }

    private void flush(String metaId, Result writer, String event, List<Map> data) {
        // 引用传递
        long total = data.size();
        long fail = writer.getFail().get();
        Meta meta = getMeta(metaId);
        meta.getFail().getAndAdd(fail);
        meta.getSuccess().getAndAdd(total - fail);

        // 记录错误数据
        Queue<Map> failData = writer.getFailData();
        boolean success = CollectionUtils.isEmpty(failData);
        if (!success) {
            data.clear();
            data.addAll(failData);
        }
        String error = writer.getError().toString();
        flushService.asyncWrite(metaId, event, success, data, error);
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
     * 获取连接配置
     *
     * @param connectorId
     * @return
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = cacheService.get(connectorId, Connector.class);
        Assert.notNull(conn, "Connector can not be null.");
        Connector connector = new Connector();
        BeanUtils.copyProperties(conn, connector);
        return connector.getConfig();
    }

    /**
     * 批量写入
     *
     * @param connectorMapper
     * @param command
     * @param fields
     * @param target
     * @param threadSize
     * @param batchSize
     * @return
     */
    private Result writeBatch(ConnectorMapper connectorMapper, Map<String, String> command, List<Field> fields, List<Map> target,
                              int threadSize, int batchSize) {
        // 总数
        int total = target.size();
        // 单次任务
        if (total <= batchSize) {
            return connectorFactory.writer(new WriterBatchConfig(connectorMapper, command, fields, target));
        }

        // 批量任务, 拆分
        int taskSize = total % batchSize == 0 ? total / batchSize : total / batchSize + 1;
        threadSize = taskSize <= threadSize ? taskSize : threadSize;

        // 转换为消息队列，根据batchSize获取数据，并发写入
        Queue<Map> queue = new ConcurrentLinkedQueue<>(target);

        // 创建线程池
        final ThreadPoolTaskExecutor executor = getThreadPoolTaskExecutor(threadSize, taskSize - threadSize);
        final Result result = new Result();
        for (; ; ) {
            if (taskSize <= 0) {
                break;
            }
            // TODO 优化 CountDownLatch
            final CountDownLatch latch = new CountDownLatch(threadSize);
            for (int i = 0; i < threadSize; i++) {
                executor.execute(() -> {
                    try {
                        Result w = parallelTask(batchSize, queue, connectorMapper, command, fields);
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

            taskSize -= threadSize;
        }

        executor.shutdown();
        return result;
    }

    private Result parallelTask(int batchSize, Queue<Map> queue, ConnectorMapper connectorMapper, Map<String, String> command,
                                List<Field> fields) {
        List<Map> data = new ArrayList<>();
        for (int j = 0; j < batchSize; j++) {
            Map poll = queue.poll();
            if (null == poll) {
                break;
            }
            data.add(poll);
        }
        return connectorFactory.writer(new WriterBatchConfig(connectorMapper, command, fields, data));
    }

    private ThreadPoolTaskExecutor getThreadPoolTaskExecutor(int threadSize, int queueCapacity) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadSize);
        executor.setMaxPoolSize(threadSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds(30);
        executor.setAwaitTerminationSeconds(30);
        executor.setThreadNamePrefix("ParserExecutor");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

}
package org.dbsyncer.parser;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.event.FullRefreshEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.model.Task;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.config.Plugin;
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
    private ApplicationContext applicationContext;

    @Override
    public boolean alive(ConnectorConfig config) {
        return connectorFactory.isAlive(config);
    }

    @Override
    public List<String> getTable(ConnectorConfig config) {
        return connectorFactory.getTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        ConnectorConfig config = getConnectorConfig(connectorId);
        return connectorFactory.getMetaInfo(config, tableName);
    }

    @Override
    public Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup) {
        final String sourceConnectorId = mapping.getSourceConnectorId();
        final String targetConnectorId = mapping.getTargetConnectorId();
        List<FieldMapping> fieldMapping = tableGroup.getFieldMapping();
        if (CollectionUtils.isEmpty(fieldMapping)) {
            return null;
        }
        String sType = getConnectorConfig(sourceConnectorId).getConnectorType();
        String tType = getConnectorConfig(targetConnectorId).getConnectorType();
        String sTableName = tableGroup.getSourceTable().getName();
        String tTableName = tableGroup.getTargetTable().getName();
        Table sTable = new Table().setName(sTableName).setColumn(new ArrayList<>());
        Table tTable = new Table().setName(tTableName).setColumn(new ArrayList<>());
        fieldMapping.forEach(m -> {
            sTable.getColumn().add(m.getSource());
            tTable.getColumn().add(m.getTarget());
        });
        final CommandConfig sourceConfig = new CommandConfig(sType, sTable, tableGroup.getFilter());
        final CommandConfig targetConfig = new CommandConfig(tType, tTable);
        // 获取连接器同步参数
        Map<String, String> command = connectorFactory.getCommand(sourceConfig, targetConfig);
        return command;
    }

    @Override
    public long getCount(String connectorId, Map<String, String> command) {
        ConnectorConfig config = getConnectorConfig(connectorId);
        return connectorFactory.getCount(config, command);
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
    public List<FilterEnum> getFilterEnumAll() {
        return Arrays.asList(FilterEnum.values());
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return Arrays.asList(ConvertEnum.values());
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
        Map<String, String> command = tableGroup.getCommand();
        Assert.notEmpty(command, "执行命令不能为空.");
        List<FieldMapping> fieldMapping = tableGroup.getFieldMapping();
        String sTableName = tableGroup.getSourceTable().getName();
        String tTableName = tableGroup.getTargetTable().getName();
        Assert.notEmpty(fieldMapping, String.format("数据源表[%s]同步到目标源表[%s], 映射关系不能为空.", sTableName, tTableName));
        // 转换配置(默认使用全局)
        List<Convert> convert = CollectionUtils.isEmpty(tableGroup.getConvert()) ? mapping.getConvert() : tableGroup.getConvert();
        // 插件配置(默认使用全局)
        Plugin plugin = null == tableGroup.getPlugin() ? mapping.getPlugin() : tableGroup.getPlugin();
        // 获取同步字段
        Picker picker = new Picker();
        PickerUtil.pickFields(picker, fieldMapping);

        // 检查分页参数
        Map<String, String> params = getMeta(metaId).getMap();
        params.putIfAbsent(ParserEnum.PAGE_INDEX.getCode(), ParserEnum.PAGE_INDEX.getDefaultValue());
        int pageSize = mapping.getReadNum();
        int threadSize = mapping.getThreadNum();
        int batchSize = mapping.getBatchNum();

        for (; ; ) {
            if (!task.isRunning()) {
                logger.warn("任务被中止:{}", metaId);
                break;
            }

            // 1、获取数据源数据
            int pageIndex = Integer.parseInt(params.get(ParserEnum.PAGE_INDEX.getCode()));
            Result reader = connectorFactory.reader(sConfig, command, pageIndex, pageSize);
            List<Map<String, Object>> data = reader.getData();
            if (CollectionUtils.isEmpty(data)) {
                params.clear();
                logger.info("完成全量同步任务:{}, [{}] >> [{}]", metaId, sTableName, tTableName);
                break;
            }

            // 2、映射字段
            PickerUtil.pickData(picker, data);

            // 3、参数转换
            List<Map<String, Object>> target = picker.getTargetList();
            ConvertUtil.convert(convert, target);

            // 4、插件转换
            pluginFactory.convert(plugin, data, target);

            // 5、写入目标源
            Result writer = writeBatch(tConfig, command, picker.getTargetFields(), target, threadSize, batchSize);

            // 6、更新结果
            flush(task, writer, target.size());

            // 7、更新分页数
            params.put(ParserEnum.PAGE_INDEX.getCode(), String.valueOf(++pageIndex));
        }
    }

    @Override
    public void execute(Mapping mapping, TableGroup tableGroup, DataEvent dataEvent) {
        logger.info("同步数据=> dataEvent:{}", dataEvent);
        final String metaId = mapping.getMetaId();

        ConnectorConfig tConfig = getConnectorConfig(mapping.getTargetConnectorId());
        Map<String, String> command = tableGroup.getCommand();
        List<FieldMapping> fieldMapping = tableGroup.getFieldMapping();
        // 转换配置(默认使用全局)
        List<Convert> convert = CollectionUtils.isEmpty(tableGroup.getConvert()) ? mapping.getConvert() : tableGroup.getConvert();
        // 插件配置(默认使用全局)
        Plugin plugin = null == tableGroup.getPlugin() ? mapping.getPlugin() : tableGroup.getPlugin();
        // 获取同步字段
        Picker picker = new Picker();
        PickerUtil.pickFields(picker, fieldMapping);

        // 1、映射字段
        String event = dataEvent.getEvent();
        Map<String, Object> data = dataEvent.getData();
        PickerUtil.pickData(picker, data);

        // 2、参数转换
        Map<String, Object> target = picker.getTarget();
        ConvertUtil.convert(convert, target);

        // 3、插件转换
        pluginFactory.convert(plugin, event, data, target);

        // 4、写入目标源
        Result writer = connectorFactory.writer(tConfig, picker.getTargetFields(), command, event, target);

        // 5、更新结果
        flush(metaId, writer, 1);
    }

    /**
     * 更新缓存
     *
     * @param task
     * @param writer
     * @param total
     */
    private void flush(Task task, Result writer, long total) {
        flush(task.getId(), writer, total);

        // 发布刷新事件给FullExtractor
        task.setEndTime(System.currentTimeMillis());
        applicationContext.publishEvent(new FullRefreshEvent(applicationContext, task));
    }

    private void flush(String metaId, Result writer, long total) {
        // 引用传递
        long fail = writer.getFail().get();
        Meta meta = getMeta(metaId);
        meta.getFail().getAndAdd(fail);
        meta.getSuccess().getAndAdd(total - fail);
        // print process
        logger.info("任务:{}, 成功:{}, 失败:{}", metaId, meta.getSuccess(), meta.getFail());

        // TODO 记录错误日志
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
     * @param config
     * @param command
     * @param fields
     * @param target
     * @param threadSize
     * @param batchSize
     * @return
     */
    private Result writeBatch(ConnectorConfig config, Map<String, String> command, List<Field> fields, List<Map<String, Object>> target,
                                int threadSize, int batchSize) {
        // 总数
        int total = target.size();
        // 单次任务
        if (total <= batchSize) {
            return connectorFactory.writer(config, command, fields, target);
        }

        // 批量任务, 拆分
        int taskSize = total % batchSize == 0 ? total / batchSize : total / batchSize + 1;
        threadSize = taskSize <= threadSize ? taskSize : threadSize;

        // 转换为消息队列，根据batchSize获取数据，并发写入
        Queue<Map<String, Object>> queue = new ConcurrentLinkedQueue<>(target);

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
                        Result w = parallelTask(batchSize, queue, config, command, fields);
                        // CAS
                        result.getFailData().addAll(w.getFailData());
                        result.getFail().getAndAdd(w.getFail().get());
                        result.getError().append(w.getError());
                    } catch (Exception e) {
                        result.getError().append(e.getMessage()).append("\r\n");
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

    private Result parallelTask(int batchSize, Queue<Map<String, Object>> queue, ConnectorConfig config, Map<String, String> command,
                                List<Field> fields) {
        List<Map<String, Object>> data = new ArrayList<>();
        for (int j = 0; j < batchSize; j++) {
            Map<String, Object> poll = queue.poll();
            if (null == poll) {
                break;
            }
            data.add(poll);
        }
        return connectorFactory.writer(config, command, fields, data);
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
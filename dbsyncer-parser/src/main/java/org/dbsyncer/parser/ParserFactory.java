package org.dbsyncer.parser;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.model.FullConvertContext;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.spi.ConvertContext;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.event.FullRefreshEvent;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
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
import java.util.concurrent.ExecutorService;

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

    @Qualifier("webApplicationContext")
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ParserStrategy parserStrategy;

    @Override
    public ConnectorMapper connect(AbstractConnectorConfig config) {
        return connectorFactory.connect(config);
    }

    @Override
    public boolean refreshConnectorConfig(AbstractConnectorConfig config) {
        return connectorFactory.refresh(config);
    }

    @Override
    public boolean isAliveConnectorConfig(AbstractConnectorConfig config) {
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
        AbstractConnectorConfig sConnConfig = getConnectorConfig(mapping.getSourceConnectorId());
        AbstractConnectorConfig tConnConfig = getConnectorConfig(mapping.getTargetConnectorId());
        Table sourceTable = tableGroup.getSourceTable();
        Table targetTable = tableGroup.getTargetTable();
        Table sTable = new Table(sourceTable.getName(), sourceTable.getType(), sourceTable.getPrimaryKey(), new ArrayList<>());
        Table tTable = new Table(targetTable.getName(), targetTable.getType(), targetTable.getPrimaryKey(), new ArrayList<>());
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
        final CommandConfig sourceConfig = new CommandConfig(sConnConfig.getConnectorType(), sTable, sourceTable, sConnConfig, tableGroup.getFilter());
        final CommandConfig targetConfig = new CommandConfig(tConnConfig.getConnectorType(), tTable, targetTable, tConnConfig);
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
            JSONObject conn = JsonUtil.parseObject(json);
            JSONObject config = (JSONObject) conn.remove("config");
            Connector connector = JsonUtil.jsonToObj(conn.toString(), Connector.class);
            Assert.notNull(connector, "Connector can not be null.");
            String connectorType = config.getString("connectorType");
            Class<?> configClass = ConnectorEnum.getConfigClass(connectorType);
            AbstractConnectorConfig obj = (AbstractConnectorConfig) JsonUtil.jsonToObj(config.toString(), configClass);
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
    public void execute(Task task, Mapping mapping, TableGroup tableGroup, ExecutorService executorService) {
        final String metaId = task.getId();
        final String sourceConnectorId = mapping.getSourceConnectorId();
        final String targetConnectorId = mapping.getTargetConnectorId();

        AbstractConnectorConfig sConfig = getConnectorConfig(sourceConnectorId);
        Assert.notNull(sConfig, "数据源配置不能为空.");
        AbstractConnectorConfig tConfig = getConnectorConfig(targetConnectorId);
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
        String pk = PrimaryKeyUtil.findOriginalTablePrimaryKey(tableGroup.getSourceTable());

        int pageSize = mapping.getReadNum();
        int batchSize = mapping.getBatchNum();
        ConnectorMapper sConnectorMapper = connectorFactory.connect(sConfig);
        ConnectorMapper tConnectorMapper = connectorFactory.connect(tConfig);
        final String event = ConnectorConstant.OPERTION_INSERT;

        for (; ; ) {
            if (!task.isRunning()) {
                logger.warn("任务被中止:{}", metaId);
                break;
            }

            // 1、获取数据源数据
            Result reader = connectorFactory.reader(sConnectorMapper, new ReaderConfig(command, new ArrayList<>(), task.getCursor(), task.getPageIndex(), pageSize));
            List<Map> data = reader.getSuccessData();
            if (CollectionUtils.isEmpty(data)) {
                logger.info("完成全量同步任务:{}, [{}] >> [{}]", metaId, sTableName, tTableName);
                break;
            }

            // 2、映射字段
            List<Map> target = picker.pickData(data);

            // 3、参数转换
            ConvertUtil.convert(group.getConvert(), target);

            // 4、插件转换
            final FullConvertContext context = new FullConvertContext(tConnectorMapper, sTableName, tTableName, event, data, target);
            pluginFactory.convert(group.getPlugin(), context);

            // 5、写入目标源
            BatchWriter batchWriter = new BatchWriter(tConnectorMapper, command, tTableName, event, picker.getTargetFields(), target, batchSize);
            Result result = writeBatch(context, batchWriter, executorService);

            // 6、同步完成后通知插件做后置处理
            pluginFactory.postProcessAfter(group.getPlugin(), context);

            // 7、更新结果
            task.setPageIndex(task.getPageIndex() + 1);
            task.setCursor(getLastCursor(data, pk));
            result.setTableGroupId(tableGroup.getId());
            result.setTargetTableGroupName(tTableName);
            flush(task, result);

            // 8、判断尾页
            if (data.size() < pageSize) {
                logger.info("完成全量:{}, [{}] >> [{}]", metaId, sTableName, tTableName);
                break;
            }
        }
    }

    @Override
    public void execute(Mapping mapping, TableGroup tableGroup, RowChangedEvent event) {
        logger.debug("Table[{}] {}, data:{}", event.getSourceTableName(), event.getEvent(), event.getDataMap());
        parserStrategy.execute(tableGroup.getId(), event.getEvent(), event.getDataMap());
    }

    /**
     * 批量写入
     *
     * @param context
     * @param batchWriter
     * @return
     */
    @Override
    public Result writeBatch(ConvertContext context, BatchWriter batchWriter) {
        return writeBatch(context, batchWriter, taskExecutor);
    }

    /**
     * 批量写入
     *
     * @param context
     * @param batchWriter
     * @param taskExecutor
     * @return
     */
    private Result writeBatch(ConvertContext context, BatchWriter batchWriter, Executor taskExecutor) {
        final Result result = new Result();
        // 终止同步数据到目标源库
        if(context.isTerminated()){
            result.getSuccessData().addAll(batchWriter.getDataList());
            return result;
        }

        List<Map> dataList = batchWriter.getDataList();
        int batchSize = batchWriter.getBatchSize();
        String tableName = batchWriter.getTableName();
        String event = batchWriter.getEvent();
        Map<String, String> command = batchWriter.getCommand();
        List<Field> fields = batchWriter.getFields();
        // 总数
        int total = dataList.size();
        // 单次任务
        if (total <= batchSize) {
            return connectorFactory.writer(batchWriter.getConnectorMapper(), new WriterBatchConfig(tableName, event, command, fields, dataList));
        }

        // 批量任务, 拆分
        int taskSize = total % batchSize == 0 ? total / batchSize : total / batchSize + 1;

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
                    Result w = connectorFactory.writer(batchWriter.getConnectorMapper(), new WriterBatchConfig(tableName, event, command, fields, data));
                    result.addSuccessData(w.getSuccessData());
                    result.addFailData(w.getFailData());
                    result.getError().append(w.getError());
                } catch (Exception e) {
                    logger.error(e.getMessage());
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
     * @param result
     */
    private void flush(Task task, Result result) {
        flushStrategy.flushFullData(task.getId(), result, ConnectorConstant.OPERTION_INSERT);

        // 发布刷新事件给FullExtractor
        task.setEndTime(Instant.now().toEpochMilli());
        applicationContext.publishEvent(new FullRefreshEvent(applicationContext, task));
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
    private AbstractConnectorConfig getConnectorConfig(String connectorId) {
        return getConnector(connectorId).getConfig();
    }

    /**
     * 获取最新游标值
     *
     * @param data
     * @param pk
     * @return
     */
    private Object getLastCursor(List<Map> data, String pk) {
        return CollectionUtils.isEmpty(data) ? null : data.get(data.size() - 1).get(pk);
    }

}
/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.ProcessEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.enums.ProcessEnum;
import org.dbsyncer.plugin.impl.FullPluginContext;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.AbstractPluginContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/29 22:38
 */
@Component
public class ParserComponentImpl implements ParserComponent {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private ProfileComponent profileComponent;

    // 移除ApplicationContext依赖，添加ProcessEvent依赖
    @Resource
    private ProcessEvent fullProcessEvent;

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        Connector connector = profileComponent.getConnector(connectorId);
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getConfig());
        MetaInfo metaInfo = connectorFactory.getMetaInfo(connectorInstance, tableName);
        if (!CollectionUtils.isEmpty(connector.getTable())) {
            for (Table t : connector.getTable()) {
                if (t.getName().equals(tableName)) {
                    metaInfo.setTableType(t.getType());
                    metaInfo.setSql(t.getSql());
                    break;
                }
            }
        }
        return metaInfo;
    }

    @Override
    public Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup) {
        ConnectorConfig sConnConfig = getConnectorConfig(mapping.getSourceConnectorId());
        ConnectorConfig tConnConfig = getConnectorConfig(mapping.getTargetConnectorId());
        Table sourceTable = tableGroup.getSourceTable();
        Table targetTable = tableGroup.getTargetTable();
        Table sTable = sourceTable.clone().setColumn(new ArrayList<>());
        Table tTable = targetTable.clone().setColumn(new ArrayList<>());
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
        final CommandConfig sourceConfig = new CommandConfig(sConnConfig.getConnectorType(), sTable,
                connectorFactory.connect(sConnConfig), tableGroup.getFilter());
        final CommandConfig targetConfig = new CommandConfig(tConnConfig.getConnectorType(), tTable,
                connectorFactory.connect(tConnConfig), null);

        // 预构建字段列表SQL片段并缓存
        String fieldListSql = tableGroup.getCachedFieldListSql();
        if (StringUtil.isBlank(fieldListSql)) {
            ConnectorService connectorService = connectorFactory.getConnectorService(sConnConfig);
            // 使用SQL模板的引号方法
            if (connectorService instanceof AbstractDatabaseConnector) {
                AbstractDatabaseConnector dbConnector = (AbstractDatabaseConnector) connectorService;
                List<String> fieldList = sTable.getColumn().stream().map(Field::getName).collect(Collectors.toList());
                fieldListSql = dbConnector.sqlTemplate.buildQuotedStringList(fieldList);
                tableGroup.setCachedFieldListSql(fieldListSql);
                List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(sTable);
                tableGroup.setCachedPrimaryKeys(dbConnector.sqlTemplate.buildQuotedStringList(primaryKeys));
            }
        }
        // 将缓存的字段列表设置到CommandConfig中
        sourceConfig.setCachedFieldListSql(fieldListSql);

        // 将缓存的主键列表设置到CommandConfig中
        sourceConfig.setCachedPrimaryKeys(tableGroup.getCachedPrimaryKeys());

        // 获取连接器同步参数
        return connectorFactory.getCommand(sourceConfig, targetConfig);
    }


    @Override
    public long getCount(String connectorId, Map<String, String> command) {
        ConnectorInstance connectorInstance = connectorFactory.connect(getConnectorConfig(connectorId));
        return connectorFactory.getCount(connectorInstance, command);
    }

    @Override
    public void execute(Task task, Mapping mapping, TableGroup tableGroup, Executor executor) {
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
        Table sourceTable = group.getSourceTable();
        String sTableName = sourceTable.getName();
        String tTableName = group.getTargetTable().getName();
        Assert.notEmpty(fieldMapping, String.format("数据源表[%s]同步到目标源表[%s], 映射关系不能为空.", sTableName, tTableName));
        // append mapping param to command
        if (mapping.getParams() != null)
            command.putAll(mapping.getParams());
        // 获取同步字段
        Picker picker = new Picker(group);
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(sourceTable);
        final FullPluginContext context = new FullPluginContext();
        context.setSourceConnectorInstance(connectorFactory.connect(sConfig));
        context.setTargetConnectorInstance(connectorFactory.connect(tConfig));
        context.setSourceTableName(sTableName);
        context.setTargetTableName(tTableName);
        context.setEvent(ConnectorConstant.OPERTION_INSERT);
        context.setCommand(command);
        context.setBatchSize(mapping.getBatchNum());
        context.setPluginExtInfo(group.getPluginExtInfo());
        context.setForceUpdate(mapping.isForceUpdate());
        context.setSourceTable(sourceTable);
        context.setTargetFields(picker.getTargetFields());
        context.setSupportedCursor(StringUtil.isNotBlank(command.get(ConnectorConstant.OPERTION_QUERY_CURSOR)));
        context.setPageSize(mapping.getReadNum());
        context.setEnableSchemaResolver(profileComponent.getSystemConfig().isEnableSchemaResolver());
        ConnectorService sourceConnector = connectorFactory
                .getConnectorService(context.getSourceConnectorInstance().getConfig());
        picker.setSourceResolver(context.isEnableSchemaResolver() ? sourceConnector.getSchemaResolver() : null);
        // 0、插件前置处理
        pluginFactory.process(group.getPlugin(), context, ProcessEnum.BEFORE);

        // 流式处理模式
        Database db = (Database) connectorFactory
                .getConnectorService(context.getSourceConnectorInstance().getConfig());
        executeWithStreaming(task, context, db, tableGroup, executor, primaryKeys);
    }

    /**
     * 流式处理模式
     */
    private void executeWithStreaming(Task task, AbstractPluginContext context, Database db, TableGroup tableGroup,
                                      Executor executor, List<String> primaryKeys) {
        final String metaId = task.getId();

        // 设置参数 - 参考 AbstractDatabaseConnector.reader 方法的逻辑
        boolean supportedCursor = context.isSupportedCursor() && null != context.getCursors();

        // 根据是否支持游标查询来设置不同的参数
        Object[] args;
        if (supportedCursor) {
            args = db.getPageCursorArgs(context);
        } else {
            args = db.getPageArgs(context);
        }

        String querySql = context.getCommand().get(ConnectorConstant.OPERTION_QUERY_STREAM);
        ((DatabaseConnectorInstance) context.getSourceConnectorInstance()).execute(databaseTemplate -> {
            // 获取流式处理的fetchSize
            Integer fetchSize = db.getStreamingFetchSize(context);
            databaseTemplate.setFetchSize(fetchSize);

            try (Stream<Map<String, Object>> stream = databaseTemplate.queryForStream(querySql,
                    new org.springframework.jdbc.core.ArgumentPreparedStatementSetter(args),
                    new org.springframework.jdbc.core.ColumnMapRowMapper())) {
                List<Map<String, Object>> batch = new ArrayList<>();
                Iterator<Map<String, Object>> iterator = stream.iterator();

                while (iterator.hasNext()) {
                    if (!task.isRunning()) {
                        logger.warn("任务被中止:{}", metaId);
                        break;
                    }

                    batch.add(iterator.next());

                    // 达到批次大小时处理数据
                    if (batch.size() >= context.getBatchSize()) {
                        processDataBatch(batch, task, context, tableGroup, executor, primaryKeys);
                        batch = new ArrayList<>();
                    }
                }

                // 处理最后一批数据
                if (!batch.isEmpty()) {
                    processDataBatch(batch, task, context, tableGroup, executor, primaryKeys);
                }
            }
            return null;
        });
    }

    /**
     * 抽离：数据处理逻辑（从现有代码中提取）
     */
    private void processDataBatch(List<Map<String, Object>> source, Task task, AbstractPluginContext context,
                                  TableGroup tableGroup, Executor executor, List<String> primaryKeys) {
        final String tTableName = context.getTargetTableName();

        // 1、映射字段
        Picker picker = new Picker(tableGroup);
        List<Map> target = picker.pickTargetData((List<Map>) (List<?>) source);

        // 2、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), target);

        // 3、插件转换
        context.setSourceList((List<Map>) (List<?>) source);
        context.setTargetList(target);
        pluginFactory.process(tableGroup.getPlugin(), context, ProcessEnum.CONVERT);

        // 4、写入目标源
        Result result = writeBatch(context, executor);

        // 5、更新结果和断点
        task.setPageIndex(task.getPageIndex() + 1);
        task.setCursors(PrimaryKeyUtil.getLastCursors((List<Map>) (List<?>) source, primaryKeys));
        result.setTableGroupId(tableGroup.getId());
        result.setTargetTableGroupName(tTableName);
        flush(task, result);

        // 6、同步完成后通知插件做后置处理
        pluginFactory.process(tableGroup.getPlugin(), context, ProcessEnum.AFTER);
    }

    @Override
    public Result writeBatch(PluginContext context, Executor executor) {
        Result result = new Result();
        // 终止同步数据到目标源库
        if (context.isTerminated()) {
            result.getSuccessData().addAll(context.getTargetList());
            return result;
        }

        int batchSize = context.getBatchSize();
        // 总数
        int total = context.getTargetList().size();
        // 单次任务
        if (total <= batchSize) {
            return connectorFactory.writer(context);
        }

        // 批量任务, 拆分
        int taskSize = total % batchSize == 0 ? total / batchSize : total / batchSize + 1;
        CountDownLatch latch = new CountDownLatch(taskSize);
        int offset = 0;
        for (int i = 0; i < taskSize; i++) {
            try {
                PluginContext tmpContext = (PluginContext) context.clone();
                tmpContext.setTargetList(
                        context.getTargetList().stream().skip(offset).limit(batchSize).collect(Collectors.toList()));
                offset += batchSize;
                executor.execute(() -> {
                    try {
                        Result w = connectorFactory.writer(tmpContext);
                        result.addSuccessData(w.getSuccessData());
                        result.addFailData(w.getFailData());
                        result.getError().append(w.getError());
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            } catch (CloneNotSupportedException e) {
                logger.error(e.getMessage(), e);
                latch.countDown();
            }
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
        // 替换事件发布为直接调用ProcessEvent的taskFinished方法
        fullProcessEvent.taskFinished(task.getId());
    }

    /**
     * 获取连接配置
     *
     * @param connectorId
     * @return
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        return profileComponent.getConnector(connectorId).getConfig();
    }

}
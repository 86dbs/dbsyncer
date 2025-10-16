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
import org.dbsyncer.sdk.connector.database.DatabaseTemplate;
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
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
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


    /**
     * 新的TableGroup流式处理方法
     */
    @Override
    public void executeTableGroup(String metaId, TableGroup tableGroup, Mapping mapping, Executor executor) {
        // 状态清理， 无须保存，因为第一个批次处理完后会保存。
        tableGroup.setErrorMessage("");

        // 上下文初始化
        final String sourceConnectorId = mapping.getSourceConnectorId();
        final String targetConnectorId = mapping.getTargetConnectorId();
        ConnectorConfig sConfig = getConnectorConfig(sourceConnectorId);
        ConnectorConfig tConfig = getConnectorConfig(targetConnectorId);
        TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
        Map<String, String> command = group.getCommand();
        Table sourceTable = group.getSourceTable();
        // append mapping param to command
        if (mapping.getParams() != null)
            command.putAll(mapping.getParams());
        // 获取同步字段
        Picker picker = new Picker(group);
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(sourceTable);

        final FullPluginContext context = new FullPluginContext();
        context.setSourceConnectorInstance(connectorFactory.connect(sConfig));
        context.setTargetConnectorInstance(connectorFactory.connect(tConfig));
        context.setSourceTableName(group.getSourceTable().getName());
        context.setSourceTable(sourceTable);
        context.setTargetTableName(group.getTargetTable().getName());
        context.setEvent(ConnectorConstant.OPERTION_INSERT);
        context.setCommand(command);
        context.setBatchSize(mapping.getBatchNum());
        context.setPlugin(group.getPlugin());
        context.setPluginExtInfo(group.getPluginExtInfo());
        context.setForceUpdate(mapping.isForceUpdate());
        context.setTargetFields(picker.getTargetFields());
        context.setPageSize(mapping.getReadNum());
        context.setEnableSchemaResolver(profileComponent.getSystemConfig().isEnableSchemaResolver());
        context.setCursors(tableGroup.getCursors());
        ConnectorService sourceConnector = connectorFactory
                .getConnectorService(context.getSourceConnectorInstance().getConfig());
        picker.setSourceResolver(context.isEnableSchemaResolver() ? sourceConnector.getSchemaResolver() : null);

        // 0、插件前置处理
        pluginFactory.process(context, ProcessEnum.BEFORE);

        // 根据是否支持cursor选择不同的SQL
        boolean supportedCursor = null != tableGroup.getCursors();
        String querySql;
        if (supportedCursor) {
            querySql = context.getCommand().get(ConnectorConstant.OPERTION_QUERY_CURSOR);
        } else {
            querySql = context.getCommand().get(ConnectorConstant.OPERTION_QUERY_STREAM);
        }

        // 流式处理模式
        Database db = (Database) connectorFactory
                .getConnectorService(context.getSourceConnectorInstance().getConfig());

        // 执行流式处理
        ((DatabaseConnectorInstance) context.getSourceConnectorInstance()).execute(databaseTemplate ->
                executeTableGroupWithStreaming(metaId, tableGroup, context, db, executor, primaryKeys, querySql, databaseTemplate));
    }

    private Object executeTableGroupWithStreaming(String metaId, TableGroup tableGroup, AbstractPluginContext context,
                                                  Database db, Executor executor, List<String> primaryKeys,
                                                  String querySql, DatabaseTemplate databaseTemplate) {
        // 获取流式处理的fetchSize
        Integer fetchSize = db.getStreamingFetchSize(context);
        databaseTemplate.setFetchSize(fetchSize);

        try (Stream<Map<String, Object>> stream = databaseTemplate.queryForStream(querySql,
                new ArgumentPreparedStatementSetter(tableGroup.getCursors()),
                new ColumnMapRowMapper())) {
            List<Map<String, Object>> batch = new ArrayList<>();
            Iterator<Map<String, Object>> iterator = stream.iterator();

            while (iterator.hasNext()) {
                // 检查任务是否被停止或线程是否被中断
                // 通过检查Meta状态来判断任务是否应该继续执行
                Meta meta = profileComponent.getMeta(metaId);
                if (!meta.isRunning() || Thread.currentThread().isInterrupted()) {
                    logger.warn("任务被中止:{}", metaId);
                    return null;
                }

                batch.add(iterator.next());

                // 达到批次大小时处理数据
                if (batch.size() >= context.getBatchSize()) {
                    processTableGroupDataBatch(metaId, batch, tableGroup, context, executor, primaryKeys);
                    batch = new ArrayList<>();
                }
            }
            // 处理最后一批数据
            if (!batch.isEmpty()) {
                processTableGroupDataBatch(metaId, batch, tableGroup, context, executor, primaryKeys);
            }
            // 标记流式处理完成
            tableGroup.setFullCompleted(true);
            profileComponent.editConfigModel(tableGroup);
        }
        return null;
    }

    /**
     * TableGroup专用的数据处理逻辑
     */
    private void processTableGroupDataBatch(String metaId, List<Map<String, Object>> source, TableGroup tableGroup,
                                            AbstractPluginContext context, Executor executor,
                                            List<String> primaryKeys) {
        // 1、映射字段
        Picker picker = new Picker(tableGroup);
        List<Map> target = picker.pickTargetData((List<Map>) (List<?>) source);

        // 2、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), target);

        // 3、插件转换
        context.setSourceList(source);
        context.setTargetList(target);
        pluginFactory.process(context, ProcessEnum.CONVERT);

        // 4、写入目标源
        if (!CollectionUtils.isEmpty(target)) {
            Result result = writeBatch(context, executor);

            // 5、更新Meta统计信息
            if (result != null) {
                result.setTargetTableGroupName(tableGroup.getName());
                flushStrategy.flushFullData(metaId, result, ConnectorConstant.OPERTION_INSERT);
            }
        }

        // 6、更新TableGroup的cursor并持久化
        Object[] newCursors = PrimaryKeyUtil.getLastCursors((List<Map>) (List<?>) source, primaryKeys);
        tableGroup.setCursors(newCursors);
        profileComponent.editConfigModel(tableGroup);

        // 7、同步完成后通知插件做后置处理
        pluginFactory.process(context, ProcessEnum.AFTER);
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
                        result.error = w.error;
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
            // 重新设置中断状态
            Thread.currentThread().interrupt();
            logger.error(e.getMessage());
        }
        return result;
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
/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.event.FullRefreshEvent;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.Task;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.enums.ProcessEnum;
import org.dbsyncer.plugin.impl.FullPluginContext;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

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

    @Resource
    private ApplicationContext applicationContext;

    @Override
    public List<MetaInfo> getMetaInfo(DefaultConnectorServiceContext context) {
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(context.getMappingId(), context.getConnectorId(), context.getSuffix());
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        return connectorFactory.getMetaInfo(connectorInstance, context);
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
            fieldMapping.forEach(m-> {
                if (null != m.getSource()) {
                    sTable.getColumn().add(m.getSource());
                }
                if (null != m.getTarget()) {
                    tTable.getColumn().add(m.getTarget());
                }
            });
        }
        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getTargetConnectorId(), ConnectorInstanceUtil.TARGET_SUFFIX);
        ConnectorInstance sourceInstance = connectorFactory.connect(sourceInstanceId);
        ConnectorInstance targetInstance = connectorFactory.connect(targetInstanceId);
        final CommandConfig sourceConfig = new CommandConfig(sConnConfig.getConnectorType(), mapping.getSourceSchema(), sTable, sourceInstance, tableGroup.getFilter());
        final CommandConfig targetConfig = new CommandConfig(tConnConfig.getConnectorType(), mapping.getTargetSchema(), tTable, targetInstance, null);
        targetConfig.setForceUpdate(mapping.isForceUpdate());
        // 获取连接器同步参数
        return connectorFactory.getCommand(sourceConfig, targetConfig);
    }

    @Override
    public void execute(Task task, Mapping mapping, TableGroup tableGroup, Executor executor) {
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
        Table sourceTable = group.getSourceTable();
        String sTableName = sourceTable.getName();
        String tTableName = group.getTargetTable().getName();
        Assert.notEmpty(fieldMapping, String.format("数据源表[%s]同步到目标源表[%s], 映射关系不能为空.", sTableName, tTableName));
        // 获取同步字段
        Picker picker = new Picker(group);
        // 游标分页时使用与构建 QUERY_CURSOR 一致的主键列表，避免 findTablePrimaryKeys 返回表上未参与游标的主键（如 id）导致 getLastCursors 多取游标值、参数个数与 SQL 占位符不一致
        boolean enableCursor = StringUtil.isNotBlank(command.get(ConnectorConstant.OPERTION_QUERY_CURSOR));
        List<String> primaryKeys = getPrimaryKeysForCursor(command, sourceTable, enableCursor);
        final FullPluginContext context = new FullPluginContext();

        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), targetConnectorId, ConnectorInstanceUtil.TARGET_SUFFIX);
        context.setSourceConnectorInstance(connectorFactory.connect(sourceInstanceId));
        context.setTargetConnectorInstance(connectorFactory.connect(targetInstanceId));
        context.setSourceTableName(sTableName);
        context.setTargetTableName(tTableName);
        context.setEvent(ConnectorConstant.OPERTION_INSERT);
        context.setCommand(command);
        context.setBatchSize(mapping.getBatchNum());
        context.setPlugin(group.getPlugin());
        context.setPluginExtInfo(group.getPluginExtInfo());
        context.setForceUpdate(mapping.isForceUpdate());
        context.setSourceTable(sourceTable);
        context.setTargetFields(picker.getTargetFields());
        context.setSupportedCursor(enableCursor);
        context.setPageSize(mapping.getReadNum());
        ConnectorService sourceConnector = connectorFactory.getConnectorService(context.getSourceConnectorInstance().getConfig());
        picker.setSourceResolver(sourceConnector.getSchemaResolver());
        // 0、插件前置处理
        pluginFactory.process(context, ProcessEnum.BEFORE);

        for (;;) {
            if (!task.isRunning()) {
                logger.warn("任务被中止:{}", metaId);
                break;
            }

            // 1、获取数据源数据
            context.setArgs(new ArrayList<>());
            context.setCursors(task.getCursors());
            context.setPageIndex(task.getPageIndex());
            Result reader = connectorFactory.reader(context);
            List<Map> source = reader.getSuccessData();
            if (CollectionUtils.isEmpty(source)) {
                logger.info("完成全量同步任务:{}, [{}] >> [{}]", metaId, sTableName, tTableName);
                break;
            }

            // 2、映射字段
            List<Map> target = picker.pickTargetData(source);

            // 3、参数转换
            ConvertUtil.convert(group.getConvert(), target);

            // 4、插件转换
            context.setSourceList(source);
            context.setTargetList(target);
            pluginFactory.process(context, ProcessEnum.CONVERT);

            // 5、写入目标源
            Result result = writeBatch(context, executor);

            // 6、更新结果
            task.setPageIndex(task.getPageIndex() + 1);
            task.setCursors(PrimaryKeyUtil.getLastCursors(source, primaryKeys));
            result.setTableGroupId(tableGroup.getId());
            result.setTargetTableGroupName(tTableName);
            flush(task, result);

            // 7、同步完成后通知插件做后置处理
            pluginFactory.process(context, ProcessEnum.AFTER);

            // 8、判断尾页（必须在 clear 之前用本批条数判断，因 source 与 getSourceList() 同一引用，clear 后 size 会变 0）
            int sourceSize = source.size();
            // 释放本批数据引用，避免 context 长期持有大 List（如 10000 条），减轻 LinkedList 保留内存
            if (context.getSourceList() != null) {
                context.getSourceList().clear();
            }
            if (context.getTargetList() != null) {
                context.getTargetList().clear();
            }
            if (sourceSize < context.getPageSize()) {
                logger.info("完成全量:{}, [{}] >> [{}]", metaId, sTableName, tTableName);
                break;
            }
        }
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
                tmpContext.setTargetList(context.getTargetList().stream().skip(offset).limit(batchSize).collect(Collectors.toList()));
                offset += batchSize;
                executor.execute(()-> {
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
        applicationContext.publishEvent(new FullRefreshEvent(applicationContext, task));
    }

    /**
     * 游标分页时使用与构建 QUERY_CURSOR 一致的主键列表；非游标或未配置时使用表主键。
     * 避免 findTablePrimaryKeys(sourceTable) 返回表上全部主键（含未参与游标的 id）导致 getLastCursors 多取游标值、参数与 SQL 占位符不一致。
     *
     * @param command    执行命令（含 CURSOR_PK_NAMES 时表示游标实际使用的主键）
     * @param sourceTable 源表
     * @param enableCursor 是否支持游标
     * @return 用于 getLastCursors 的主键名列表
     */
    private List<String> getPrimaryKeysForCursor(Map<String, String> command, Table sourceTable, boolean enableCursor) {
        if (enableCursor) {
            String cursorPkNames = command.get(ConnectorConstant.CURSOR_PK_NAMES);
            if (StringUtil.isNotBlank(cursorPkNames)) {
                return Arrays.stream(cursorPkNames.split(StringUtil.COMMA)).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
            }
        }
        // 不支持游标查询，或非结构化连接器场景
        return PrimaryKeyUtil.findTablePrimaryKeys(sourceTable);
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
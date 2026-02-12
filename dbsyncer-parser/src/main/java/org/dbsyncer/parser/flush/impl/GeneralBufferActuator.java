/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.config.GeneralBufferConfig;
import org.dbsyncer.common.metric.TimeRegistry;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupContext;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.event.RefreshOffsetEvent;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.TableGroupPicker;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.model.WriterResponse;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.enums.ProcessEnum;
import org.dbsyncer.plugin.impl.IncrementPluginContext;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * 通用执行器（单线程消费，多线程批量写，按序执行）
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2022-03-27 16:50
 */
@Component
public class GeneralBufferActuator extends AbstractBufferActuator<WriterRequest, WriterResponse> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private GeneralBufferConfig generalBufferConfig;

    @Resource
    private Executor generalExecutor;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private DDLParser ddlParser;

    @Resource
    private TableGroupContext tableGroupContext;

    @PostConstruct
    public void init() {
        setConfig(generalBufferConfig);
        buildConfig();
    }

    @Override
    protected String getPartitionKey(WriterRequest request) {
        return request.getTableName();
    }

    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        if (!CollectionUtils.isEmpty(request.getRow())) {
            response.addData(request.getRow());
        }
        if (request.getChangedOffset() != null) {
            response.setChangedOffset(request.getChangedOffset());
        }
        if (!response.isMerged()) {
            response.setTraceId(request.getTraceId());
            response.setTableName(request.getTableName());
            response.setEvent(request.getEvent());
            response.setTypeEnum(request.getTypeEnum());
            response.setSql(request.getSql());
            response.setMerged(true);
        } else if (profileComponent.getSystemConfig().isEnablePrintTraceInfo() && StringUtil.isNotBlank(request.getTraceId())) {
            logger.info("traceId:{} merge into traceId:{}", request.getTraceId(), response.getTraceId());
        }
    }

    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // 并发场景，同一条数据可能连续触发Insert > Delete > Insert，批处理任务中出现不同事件时，跳过分区处理
        // 跳过表结构修改事件（保证表结构修改原子性）
        return !StringUtil.equals(nextRequest.getEvent(), response.getEvent()) || ChangedEventTypeEnum.isDDL(response.getTypeEnum());
    }

    @Override
    public void pull(WriterResponse response) {
        Meta meta = profileComponent.getMeta(response.getChangedOffset().getMetaId());
        if (meta == null) {
            return;
        }
        // 打印trace信息
        printTraceInfo(response);
        final Mapping mapping = profileComponent.getMapping(meta.getMappingId());
        List<TableGroupPicker> pickers = tableGroupContext.getTableGroupPickers(meta.getId(), response.getTableName());

        switch (response.getTypeEnum()) {
            case DDL:
                tableGroupContext.update(mapping, pickers.stream().map(picker-> {
                    TableGroup tableGroup = profileComponent.getTableGroup(picker.getTableGroup().getId());
                    parseDDl(response, mapping, tableGroup);
                    return tableGroup;
                }).collect(Collectors.toList()));
                break;
            case SCAN:
                pickers.forEach(picker->distributeTableGroup(response, mapping, picker, picker.getSourceFields(), false));
                break;
            case ROW:
                pickers.forEach(picker->distributeTableGroup(response, mapping, picker, picker.getTableGroup().getSourceTable().getColumn(), true));
                // 发布刷新增量点事件
                applicationContext.publishEvent(new RefreshOffsetEvent(applicationContext, response.getChangedOffset()));
                break;
            default:
                break;
        }
        // 及时清空列表引用，便于 GC 回收，减轻 parser 侧 LinkedList 保留内存
        response.getDataList().clear();
    }

    @Override
    protected void offerFailed(BlockingQueue<WriterRequest> queue, WriterRequest request) {
        throw new QueueOverflowException("缓存队列已满");
    }

    @Override
    protected void meter(TimeRegistry timeRegistry, long count) {
        // 统计执行器同步效率TPS
        timeRegistry.meter(TimeRegistry.GENERAL_BUFFER_ACTUATOR_TPS).add(count);
    }

    @Override
    public Executor getExecutor() {
        return generalExecutor;
    }

    private void distributeTableGroup(WriterResponse response, Mapping mapping, TableGroupPicker tableGroupPicker, List<Field> sourceFields, boolean enableFilter) {
        // 1、映射字段
        ConnectorConfig sourceConfig = getConnectorConfig(mapping.getSourceConnectorId());
        ConnectorService sourceConnector = connectorFactory.getConnectorService(sourceConfig.getConnectorType());
        List<Map> sourceDataList = new ArrayList<>();
        List<Map> targetDataList = tableGroupPicker.getPicker().setSourceResolver(sourceConnector.getSchemaResolver())
                .pickTargetData(sourceFields, enableFilter, response.getDataList(), sourceDataList);
        if (CollectionUtils.isEmpty(targetDataList)) {
            return;
        }

        // 2、参数转换
        TableGroup tableGroup = tableGroupPicker.getTableGroup();
        ConvertUtil.convert(tableGroup.getConvert(), targetDataList);

        // 3、插件转换
        final IncrementPluginContext context = new IncrementPluginContext();
        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getTargetConnectorId(), ConnectorInstanceUtil.TARGET_SUFFIX);
        context.setSourceConnectorInstance(connectorFactory.connect(sourceInstanceId));
        context.setTargetConnectorInstance(connectorFactory.connect(targetInstanceId));
        context.setSourceTable(tableGroup.getSourceTable());
        context.setSourceTableName(tableGroup.getSourceTable().getName());
        context.setTargetTableName(tableGroup.getTargetTable().getName());
        context.setTraceId(response.getTraceId());
        context.setEvent(response.getEvent());
        context.setTargetFields(tableGroupPicker.getTargetFields());
        context.setCommand(tableGroup.getCommand());
        context.setBatchSize(getBufferWriterCount());
        context.setSourceList(sourceDataList);
        context.setTargetList(targetDataList);
        context.setPlugin(tableGroup.getPlugin());
        context.setPluginExtInfo(tableGroup.getPluginExtInfo());
        context.setForceUpdate(mapping.isForceUpdate());
        context.setEnablePrintTraceInfo(StringUtil.isNotBlank(response.getTraceId()));
        pluginFactory.process(context, ProcessEnum.CONVERT);

        // 4、批量执行同步
        Result result = parserComponent.writeBatch(context, getExecutor());

        // 5、持久化同步结果
        result.setTableGroupId(tableGroup.getId());
        result.setTargetTableGroupName(context.getTargetTableName());
        flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());

        // 6、执行后置处理
        pluginFactory.process(context, ProcessEnum.AFTER);
    }

    /**
     * 解析DDL
     */
    private void parseDDl(WriterResponse response, Mapping mapping, TableGroup tableGroup) {
        try {
            ConnectorConfig sConnConfig = getConnectorConfig(mapping.getSourceConnectorId());
            ConnectorConfig tConnConfig = getConnectorConfig(mapping.getTargetConnectorId());
            String sConnType = sConnConfig.getConnectorType();
            String tConnType = tConnConfig.getConnectorType();
            ConnectorService connectorService = connectorFactory.getConnectorService(tConnType);
            DDLConfig targetDDLConfig = ddlParser.parse(connectorService, tableGroup, response.getSql());
            // 1.生成目标表执行SQL(暂支持同源)
            if (mapping.getListener().isEnableDDL() && StringUtil.equals(sConnType, tConnType)) {
                String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getTargetConnectorId(), ConnectorInstanceUtil.TARGET_SUFFIX);
                ConnectorInstance tConnectorInstance = connectorFactory.connect(instanceId);
                Result result = connectorFactory.writerDDL(tConnectorInstance, targetDDLConfig);
                // 2.持久化增量事件数据
                result.setTableGroupId(tableGroup.getId());
                result.setTargetTableGroupName(tableGroup.getTargetTable().getName());
                flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());
            }

            // 3.更新表属性字段
            updateTableColumn(mapping, ConnectorInstanceUtil.SOURCE_SUFFIX, tableGroup.getSourceTable());
            updateTableColumn(mapping, ConnectorInstanceUtil.TARGET_SUFFIX, tableGroup.getTargetTable());

            // 4.更新表字段映射关系
            ddlParser.refreshFiledMappings(tableGroup, targetDDLConfig);

            // 5.更新执行命令
            tableGroup.setCommand(parserComponent.getCommand(mapping, tableGroup));

            // 6.持久化存储 & 更新缓存配置
            profileComponent.editTableGroup(tableGroup);

            // 7.发布更新事件
            applicationContext.publishEvent(new RefreshOffsetEvent(applicationContext, response.getChangedOffset()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void updateTableColumn(Mapping mapping, String suffix, Table table) {
        boolean isSource = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(mapping, isSource);
        context.addTablePattern(table);

        List<MetaInfo> metaInfos = parserComponent.getMetaInfo(context);
        MetaInfo metaInfo = CollectionUtils.isEmpty(metaInfos) ? null : metaInfos.get(0);
        Assert.notNull(metaInfo, "无法获取连接器表信息:" + table.getName());
        table.setColumn(metaInfo.getColumn());
    }

    /**
     * 获取连接器配置
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = profileComponent.getConnector(connectorId);
        Assert.notNull(conn, "Connector can not be null.");
        return conn.getConfig();
    }

    private void printTraceInfo(WriterResponse response) {
        if (profileComponent.getSystemConfig().isEnablePrintTraceInfo() && StringUtil.isNotBlank(response.getTraceId())) {
            logger.info("traceId:{}, tableName:{}, event:{}, offset:{}, row:{}", response.getTraceId(), response.getTableName(), response.getEvent(), JsonUtil
                    .objToJson(response.getChangedOffset()), response.getDataList());
        }
    }

}
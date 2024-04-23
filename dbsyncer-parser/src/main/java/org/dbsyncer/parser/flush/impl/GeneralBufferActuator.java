/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.config.GeneralBufferConfig;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.event.RefreshOffsetEvent;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.BatchWriter;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.model.WriterResponse;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.impl.IncrementPluginContext;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.MetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

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
    protected ProfileComponent profileComponent;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private DDLParser ddlParser;

    @PostConstruct
    public void init() {
        setConfig(generalBufferConfig);
        buildConfig();
    }

    @Override
    protected String getPartitionKey(WriterRequest request) {
        return request.getTableGroupId();
    }

    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        if (!CollectionUtils.isEmpty(request.getRow())) {
            response.addRow(request.getRow());
        }
        if (request.getChangedOffset() != null) {
            response.addChangedOffset(request.getChangedOffset());
        }
        if (!response.isMerged()) {
            response.setTableGroupId(request.getTableGroupId());
            response.setEvent(request.getEvent());
            response.setTypeEnum(request.getTypeEnum());
            response.setSql(request.getSql());
            response.setMerged(true);
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
        // 0、获取配置信息
        final TableGroup tableGroup = getTableGroup(response.getTableGroupId());
        final Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
        final TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);

        // 1、ddl解析
        if (ChangedEventTypeEnum.isDDL(response.getTypeEnum())) {
            parseDDl(response, mapping, group);
            return;
        }

        final String sourceTableName = group.getSourceTable().getName();
        final String targetTableName = group.getTargetTable().getName();
        final String event = response.getEvent();
        final Picker picker = new Picker(group.getFieldMapping());
        final List<Map> sourceDataList = response.getDataList();
        // 2、映射字段
        List<Map> targetDataList = picker.pickTargetData(sourceDataList);

        // 3、参数转换
        ConvertUtil.convert(group.getConvert(), targetDataList);

        // 4、插件转换
        final ConnectorInstance sConnectorInstance = connectorFactory.connect(getConnectorConfig(mapping.getSourceConnectorId()));
        final ConnectorInstance tConnectorInstance = connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId()));
        final IncrementPluginContext context = new IncrementPluginContext(sConnectorInstance, tConnectorInstance, sourceTableName, targetTableName, event, sourceDataList, targetDataList);
        pluginFactory.convert(group.getPlugin(), context);
        boolean enableOnlyUpdate = mapping.isEnableOnlyUpdate()?true:profileComponent.getSystemConfig().isEnableOnlyUpdate();

        // 5、批量执行同步
        BatchWriter batchWriter = new BatchWriter(tConnectorInstance, group.getCommand(), targetTableName, event, picker.getTargetFields(), targetDataList, generalBufferConfig.getBufferWriterCount(),enableOnlyUpdate);
        Result result = parserComponent.writeBatch(context, batchWriter, getExecutor());

        // 6.发布刷新增量点事件
        applicationContext.publishEvent(new RefreshOffsetEvent(applicationContext, response.getOffsetList()));

        // 7、持久化同步结果
        result.setTableGroupId(tableGroup.getId());
        result.setTargetTableGroupName(targetTableName);
        flushStrategy.flushIncrementData(mapping.getMetaId(), result, event);

        // 8、执行批量处理后的
        pluginFactory.postProcessAfter(group.getPlugin(), context);
    }

    @Override
    protected void offerFailed(BlockingQueue<WriterRequest> queue, WriterRequest request) {
        throw new QueueOverflowException("缓存队列已满");
    }

    @Override
    public Executor getExecutor() {
        return generalExecutor;
    }

    public TableGroup getTableGroup(String tableGroupId) {
        return profileComponent.getTableGroup(tableGroupId);
    }

    /**
     * 解析DDL
     *
     * @param response
     * @param mapping
     * @param tableGroup
     */
    private void parseDDl(WriterResponse response, Mapping mapping, TableGroup tableGroup) {
        try {
            ConnectorConfig sConnConfig = getConnectorConfig(mapping.getSourceConnectorId());
            ConnectorConfig tConnConfig = getConnectorConfig(mapping.getTargetConnectorId());
            String sConnType = sConnConfig.getConnectorType();
            String tConnType = tConnConfig.getConnectorType();
            // 0.生成目标表执行SQL(暂支持同源)
            if (StringUtil.equals(sConnType, tConnType)) {
                // 1.转换为目标SQL，执行到目标库
                String targetTableName = tableGroup.getTargetTable().getName();
                List<FieldMapping> originalFieldMappings = tableGroup.getFieldMapping();
                DDLConfig targetDDLConfig = ddlParser.parseDDlConfig(response.getSql(), tConnType, targetTableName, originalFieldMappings);
                final ConnectorInstance tConnectorInstance = connectorFactory.connect(tConnConfig);
                Result result = connectorFactory.writerDDL(tConnectorInstance, targetDDLConfig);
                result.setTableGroupId(tableGroup.getId());
                result.setTargetTableGroupName(targetTableName);

                // 2.获取目标表最新的属性字段
                MetaInfo targetMetaInfo = parserComponent.getMetaInfo(mapping.getTargetConnectorId(), targetTableName);
                MetaInfo originMetaInfo = parserComponent.getMetaInfo(mapping.getSourceConnectorId(), tableGroup.getSourceTable().getName());

                // 3.更新表字段映射(根据保留的更改的属性，进行更改)
                tableGroup.getSourceTable().setColumn(originMetaInfo.getColumn());
                tableGroup.getTargetTable().setColumn(targetMetaInfo.getColumn());
                tableGroup.setFieldMapping(ddlParser.refreshFiledMappings(originalFieldMappings, originMetaInfo, targetMetaInfo, targetDDLConfig));

                // 4.更新执行命令
                Map<String, String> commands = parserComponent.getCommand(mapping, tableGroup);
                tableGroup.setCommand(commands);

                // 5.持久化存储 & 更新缓存配置
                profileComponent.editTableGroup(tableGroup);

                // 6.发布更新事件，持久化增量数据
                applicationContext.publishEvent(new RefreshOffsetEvent(applicationContext, response.getOffsetList()));
                flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());
                return;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return;
        }
        logger.warn("暂只支持数据库同源并且是关系性解析DDL");
    }

    /**
     * 获取连接器配置
     *
     * @param connectorId
     * @return
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = profileComponent.getConnector(connectorId);
        Assert.notNull(conn, "Connector can not be null.");
        return conn.getConfig();
    }

}
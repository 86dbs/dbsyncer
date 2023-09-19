package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.config.GeneralBufferConfig;
import org.dbsyncer.common.event.RefreshOffsetEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.model.IncrementConvertContext;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.parser.ParserFactory;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.BatchWriter;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.model.WriterResponse;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * 通用执行器（单线程消费，多线程批量写，按序执行）
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public class GeneralBufferActuator extends AbstractBufferActuator<WriterRequest, WriterResponse> {

    @Resource
    private GeneralBufferConfig generalBufferConfig;

    @Resource
    private Executor generalExecutor;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private ParserFactory parserFactory;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private CacheService cacheService;

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
            response.getDataList().add(request.getRow());
        }
        response.getOffsetList().add(request.getChangedOffset());
        if (!response.isMerged()) {
            response.setTableGroupId(request.getTableGroupId());
            response.setEvent(request.getEvent());
            response.setSql(request.getSql());
            response.setMerged(true);
        }
    }

    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // 并发场景，同一条数据可能连续触发Insert > Delete > Insert，批处理任务中出现不同事件时，跳过分区处理
        // 跳过表结构修改事件（保证表结构修改原子性）
        return !StringUtil.equals(nextRequest.getEvent(), response.getEvent()) || isDDLEvent(response.getEvent());
    }

    @Override
    protected void pull(WriterResponse response) {
        // 0、获取配置信息
        final TableGroup tableGroup = cacheService.get(response.getTableGroupId(), TableGroup.class);
        final Mapping mapping = cacheService.get(tableGroup.getMappingId(), Mapping.class);
        final TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
        final String sourceTableName = group.getSourceTable().getName();
        final String targetTableName = group.getTargetTable().getName();
        final String event = response.getEvent();
        final Picker picker = new Picker(group.getFieldMapping());
        final List<Map> sourceDataList = response.getDataList();

        // 1、ddl解析
        if (isDDLEvent(response.getEvent())) {
            parseDDl(response, mapping, group);
            return;
        }

        // 2、映射字段
        List<Map> targetDataList = picker.pickData(sourceDataList);

        // 3、参数转换
        ConvertUtil.convert(group.getConvert(), targetDataList);

        // 4、插件转换
        final ConnectorMapper sConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getSourceConnectorId()));
        final ConnectorMapper tConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId()));
        final IncrementConvertContext context = new IncrementConvertContext(sConnectorMapper, tConnectorMapper, sourceTableName, targetTableName, event, sourceDataList, targetDataList);
        pluginFactory.convert(group.getPlugin(), context);

        // 5、批量执行同步
        BatchWriter batchWriter = new BatchWriter(tConnectorMapper, group.getCommand(), targetTableName, event, picker.getTargetFields(), targetDataList, generalBufferConfig.getBufferWriterCount());
        Result result = parserFactory.writeBatch(context, batchWriter, generalExecutor);

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
    public Executor getExecutor() {
        return generalExecutor;
    }

    private boolean isDDLEvent(String event) {
        return StringUtil.equals(event, ConnectorConstant.OPERTION_ALTER);
    }

    /**
     * 解析DDL
     *
     * @param response
     * @param mapping
     * @param group
     */
    private void parseDDl(WriterResponse response, Mapping mapping, TableGroup group) {
        AbstractConnectorConfig sConnConfig = getConnectorConfig(mapping.getSourceConnectorId());
        AbstractConnectorConfig tConnConfig = getConnectorConfig(mapping.getTargetConnectorId());
        // 0.生成目标表执行SQL(暂支持MySQL) fixme AE86 暂内测MySQL作为试运行版本
        if (StringUtil.equals(sConnConfig.getConnectorType(), tConnConfig.getConnectorType()) && StringUtil.equals(ConnectorEnum.MYSQL.getType(), tConnConfig.getConnectorType())) {
            final String targetTableName = group.getTargetTable().getName();
            DDLConfig targetDDLConfig = ddlParser.parseDDlConfig(response.getSql(), targetTableName);
            final ConnectorMapper tConnectorMapper = connectorFactory.connect(tConnConfig);
            Result result = connectorFactory.writerDDL(tConnectorMapper, targetDDLConfig);
            result.setTableGroupId(group.getId());
            result.setTargetTableGroupName(targetTableName);
            applicationContext.publishEvent(new RefreshOffsetEvent(applicationContext, response.getOffsetList()));
            flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());
        }
        // TODO life
        // 1.获取目标表最新的属性字段
        // 2.更新TableGroup.targetTable
        // 3.更新表字段映射（添加相似字段）
        // 4.更新TableGroup.command
        // 5.合并驱动配置
    }

    /**
     * 获取连接器配置
     *
     * @param connectorId
     * @return
     */
    private AbstractConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = cacheService.get(connectorId, Connector.class);
        Assert.notNull(conn, "Connector can not be null.");
        return conn.getConfig();
    }

    public void setGeneralExecutor(Executor generalExecutor) {
        this.generalExecutor = generalExecutor;
    }
}
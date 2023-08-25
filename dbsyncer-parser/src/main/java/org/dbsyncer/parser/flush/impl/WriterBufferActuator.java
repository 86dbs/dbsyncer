package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.event.RefreshOffsetEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.model.IncrementConvertContext;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.parser.ParserFactory;
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
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * 同步任务缓冲执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
public class WriterBufferActuator extends AbstractBufferActuator<WriterRequest, WriterResponse> {

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

    @Override
    protected String getPartitionKey(WriterRequest request) {
        return request.getTableGroupId();
    }

    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        response.getDataList().add(request.getRow());
        response.getOffsetList().add(request.getChangedOffset());
        if (!response.isMerged()) {
            response.setTableGroupId(request.getTableGroupId());
            response.setEvent(request.getEvent());
            response.setMerged(true);
        }
    }

    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // 并发场景，同一条数据可能连续触发Insert > Delete > Insert，批处理任务中出现不同事件时，跳过分区处理
        return !StringUtil.equals(nextRequest.getEvent(), response.getEvent());
    }

    @Override
    protected void pull(WriterResponse response) {
        // 1、获取配置信息
        final TableGroup tableGroup = cacheService.get(response.getTableGroupId(), TableGroup.class);
        final Mapping mapping = cacheService.get(tableGroup.getMappingId(), Mapping.class);
        final TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
        final String sourceTableName = group.getSourceTable().getName();
        final String targetTableName = group.getTargetTable().getName();
        final String event = response.getEvent();
        final Picker picker = new Picker(group.getFieldMapping());
        final List<Map> sourceDataList = response.getDataList();

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
        BatchWriter batchWriter = new BatchWriter(tConnectorMapper, group.getCommand(), targetTableName, event, picker.getTargetFields(), targetDataList, getConfig().getWriterBatchCount());
        Result result = parserFactory.writeBatch(context, batchWriter);

        // 6.发布刷新增量点事件
        applicationContext.publishEvent(new RefreshOffsetEvent(applicationContext, response.getOffsetList()));

        // 7、持久化同步结果
        result.setTableGroupId(tableGroup.getId());
        result.setTargetTableGroupName(targetTableName);
        flushStrategy.flushIncrementData(mapping.getMetaId(), result, event);

        // 8、执行批量处理后的
        pluginFactory.postProcessAfter(group.getPlugin(), context);
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
}
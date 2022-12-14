package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.config.BufferActuatorConfig;
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
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public class WriterBufferActuator extends AbstractBufferActuator<WriterRequest, WriterResponse> {

    @Autowired
    private ConnectorFactory connectorFactory;

    @Autowired
    private ParserFactory parserFactory;

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private FlushStrategy flushStrategy;

    @Autowired
    private ParserStrategy parserStrategy;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private BufferActuatorConfig bufferActuatorConfig;

    @Override
    protected String getPartitionKey(WriterRequest request) {
        return request.getTableGroupId();
    }

    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        response.getDataList().add(request.getRow());
        if (StringUtil.isNotBlank(request.getMessageId())) {
            response.getMessageIds().add(request.getMessageId());
        }
        if (response.isMerged()) {
            return;
        }
        response.setTableGroupId(request.getTableGroupId());
        response.setEvent(request.getEvent());
        response.setMerged(true);
    }

    @Override
    protected void pull(WriterResponse response) {
        // 1?????????????????????
        final TableGroup tableGroup = cacheService.get(response.getTableGroupId(), TableGroup.class);
        final Mapping mapping = cacheService.get(tableGroup.getMappingId(), Mapping.class);
        final TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
        final String sourceTableName = group.getSourceTable().getName();
        final String targetTableName = group.getTargetTable().getName();
        final String event = response.getEvent();
        final List<Map> sourceDataList = response.getDataList();

        // 2???????????????
        final Picker picker = new Picker(group.getFieldMapping());
        List<Map> targetDataList = picker.pickData(sourceDataList);

        // 3???????????????
        ConvertUtil.convert(group.getConvert(), targetDataList);

        // 4???????????????
        final ConnectorMapper sConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getSourceConnectorId()));
        final ConnectorMapper tConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId()));
        final IncrementConvertContext context = new IncrementConvertContext(sConnectorMapper, tConnectorMapper, sourceTableName, targetTableName, event, sourceDataList, targetDataList);
        pluginFactory.convert(group.getPlugin(), context);

        // 5?????????????????????
        Result result = parserFactory.writeBatch(context, new BatchWriter(tConnectorMapper, group.getCommand(), targetTableName, event,
                picker.getTargetFields(), targetDataList, bufferActuatorConfig.getWriterBatchCount()));

        // 6????????????????????????
        result.setTableGroupId(tableGroup.getId());
        result.setTargetTableGroupName(targetTableName);
        flushStrategy.flushIncrementData(mapping.getMetaId(), result, event);

        // 7???????????????????????????
        pluginFactory.postProcessAfter(group.getPlugin(), context);

        // 8???????????????
        parserStrategy.complete(response.getMessageIds());
    }

    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // ????????????????????????????????????????????????Insert > Delete > Insert???????????????????????????????????????????????????????????????
        return !StringUtil.equals(nextRequest.getEvent(), response.getEvent());
    }

    /**
     * ?????????????????????
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
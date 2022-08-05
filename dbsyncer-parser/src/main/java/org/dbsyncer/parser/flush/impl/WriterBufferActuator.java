package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.parser.ParserFactory;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

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
        if(StringUtil.isNotBlank(request.getMessageId())){
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
        // 1、获取配置信息
        final TableGroup tableGroup = cacheService.get(response.getTableGroupId(), TableGroup.class);
        final Mapping mapping = cacheService.get(tableGroup.getMappingId(), Mapping.class);
        final String targetTableName = tableGroup.getTargetTable().getName();
        final Picker picker = new Picker(tableGroup.getFieldMapping());

        // 2、批量执行同步
        ConnectorMapper targetConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId()));
        Result result = parserFactory.writeBatch(new BatchWriter(targetConnectorMapper, tableGroup.getCommand(), targetTableName, response.getEvent(),
                picker.getTargetFields(), response.getDataList(), bufferActuatorConfig.getWriterBatchCount()));

        // 3、持久化同步结果
        flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());

        // 4、消息处理完成
        parserStrategy.complete(response.getMessageIds());
    }

    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // 并发场景，同一条数据可能连续触发Insert > Delete > Insert，批处理任务中出现不同事件时，跳过分区处理
        return !StringUtil.equals(nextRequest.getEvent(), response.getEvent());
    }

    /**
     * 获取连接器配置
     *
     * @param connectorId
     * @return
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = cacheService.get(connectorId, Connector.class);
        Assert.notNull(conn, "Connector can not be null.");
        return conn.getConfig();
    }
}
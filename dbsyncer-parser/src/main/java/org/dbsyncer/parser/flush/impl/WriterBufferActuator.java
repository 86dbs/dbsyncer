package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.parser.ParserFactory;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.flush.FlushStrategy;
import org.dbsyncer.parser.flush.model.AbstractResponse;
import org.dbsyncer.parser.flush.model.WriterRequest;
import org.dbsyncer.parser.flush.model.WriterResponse;
import org.dbsyncer.parser.model.BatchWriter;
import org.dbsyncer.parser.model.Connector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Collections;

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
    private CacheService cacheService;

    private final static int BATCH_SIZE = 100;

    @Override
    protected long getPeriod() {
        return 300;
    }

    @Override
    protected AbstractResponse getValue() {
        return new WriterResponse();
    }

    @Override
    protected String getPartitionKey(WriterRequest request) {
        return new StringBuilder(request.getTableGroupId()).append("-").append(request.getEvent()).toString();
    }

    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        response.getDataList().add(request.getRow());
        if (response.isMerged()) {
            return;
        }
        response.setMetaId(request.getMetaId());
        response.setTargetConnectorId(request.getTargetConnectorId());
        response.setCommand(request.getCommand());
        response.setTableName(request.getTableName());
        response.setEvent(request.getEvent());
        response.setFields(Collections.unmodifiableList(request.getFields()));
        response.setMerged(true);
    }

    @Override
    protected void pull(WriterResponse response) {
        ConnectorMapper targetConnectorMapper = connectorFactory.connect(getConnectorConfig(response.getTargetConnectorId()));
        Result result = parserFactory.writeBatch(new BatchWriter(targetConnectorMapper, response.getCommand(), response.getTableName(), response.getEvent(),
                response.getFields(), response.getDataList(), BATCH_SIZE, true));
        flushStrategy.flushIncrementData(response.getMetaId(), result, response.getEvent());
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
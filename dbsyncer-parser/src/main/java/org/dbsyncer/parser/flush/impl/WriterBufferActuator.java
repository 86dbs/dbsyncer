package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.ParserFactory;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.flush.FlushStrategy;
import org.dbsyncer.parser.flush.model.AbstractResponse;
import org.dbsyncer.parser.flush.model.WriterRequest;
import org.dbsyncer.parser.flush.model.WriterResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public class WriterBufferActuator extends AbstractBufferActuator<WriterRequest, WriterResponse> {

    @Autowired
    private ParserFactory parserFactory;

    @Autowired
    private FlushStrategy flushStrategy;

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
        response.setEvent(request.getEvent());
        response.setConnectorMapper(request.getConnectorMapper());
        response.setFields(Collections.unmodifiableList(request.getFields()));
        response.setCommand(request.getCommand());
        response.setMerged(true);
    }

    @Override
    protected void pull(WriterResponse response) {
        Result result = parserFactory.writeBatch(response.getConnectorMapper(), response.getCommand(), response.getEvent(),
                response.getFields(), response.getDataList(), BATCH_SIZE);
        flushStrategy.flushIncrementData(response.getMetaId(), result, response.getEvent());
    }
}
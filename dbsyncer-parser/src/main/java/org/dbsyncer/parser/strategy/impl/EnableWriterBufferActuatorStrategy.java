package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.parser.AbstractWriterBinlog;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(value = "dbsyncer.parser.flush.buffer.actuator.speed.enabled", havingValue = "true")
public final class EnableWriterBufferActuatorStrategy extends AbstractWriterBinlog implements ParserStrategy {

    private static final double BUFFER_THRESHOLD = 0.8;

    @Autowired
    private BufferActuator writerBufferActuator;

    private static double limit;

    @PostConstruct
    private void init() {
        limit = Math.ceil(getQueueCapacity() * BUFFER_THRESHOLD);
    }

    @Override
    public void execute(String tableGroupId, String event, Map<String, Object> data) {
        if (getQueue().size() >= limit) {
            super.flush(tableGroupId, event, data);
            return;
        }
        writerBufferActuator.offer(new WriterRequest(tableGroupId, event, data));
    }

    @Override
    public void complete(List<String> messageIds) {
        super.complete(messageIds);
    }

}
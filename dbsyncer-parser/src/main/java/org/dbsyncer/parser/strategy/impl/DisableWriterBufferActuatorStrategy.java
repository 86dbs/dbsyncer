package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.parser.AbstractWriterBinlog;
import org.dbsyncer.parser.strategy.ParserStrategy;

import java.util.List;
import java.util.Map;

public final class DisableWriterBufferActuatorStrategy extends AbstractWriterBinlog implements ParserStrategy {

    @Override
    public void execute(String tableGroupId, String event, Map<String, Object> data) {
        super.flush(tableGroupId, event, data);
    }

    @Override
    public void complete(List<String> messageIds) {
        super.complete(messageIds);
    }

}
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.ParserFactory;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.flush.AbstractFlushTask;
import org.dbsyncer.parser.flush.FlushStrategy;
import org.dbsyncer.parser.flush.model.WriterBufferTask;
import org.dbsyncer.parser.flush.model.WriterFlushTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public class WriterBufferActuator extends AbstractBufferActuator<WriterBufferTask, WriterFlushTask> {

    @Autowired
    private ParserFactory parserFactory;

    @Autowired
    private FlushStrategy flushStrategy;

    private final static int BATCH_SIZE = 100;

    @Override
    protected AbstractFlushTask getValue() {
        return new WriterFlushTask();
    }

    @Override
    protected String getPartitionKey(WriterBufferTask bufferTask) {
        return new StringBuilder(bufferTask.getTableGroupId()).append(bufferTask.getEvent()).toString();
    }

    @Override
    protected void partition(WriterBufferTask bufferTask, WriterFlushTask flushTask) {
        flushTask.getDataList().add(bufferTask.getRow());
        if (flushTask.isMerged()) {
            return;
        }
        flushTask.setMetaId(bufferTask.getMetaId());
        flushTask.setEvent(bufferTask.getEvent());
        flushTask.setConnectorMapper(bufferTask.getConnectorMapper());
        flushTask.setFields(Collections.unmodifiableList(bufferTask.getFields()));
        flushTask.setCommand(bufferTask.getCommand());
        flushTask.setMerged(true);
    }

    @Override
    protected void flush(WriterFlushTask flushTask) {
        Result result = parserFactory.writeBatch(flushTask.getConnectorMapper(), flushTask.getCommand(), flushTask.getEvent(), flushTask.getFields(), flushTask.getDataList(), BATCH_SIZE);
        flushStrategy.flushIncrementData(flushTask.getMetaId(), result, flushTask.getEvent(), flushTask.getDataList());
    }
}
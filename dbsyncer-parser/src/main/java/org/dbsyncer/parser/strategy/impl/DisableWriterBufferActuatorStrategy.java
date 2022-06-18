package org.dbsyncer.parser.strategy.impl;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.dbsyncer.storage.binlog.AbstractBinlogRecorder;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.Data;
import org.dbsyncer.storage.binlog.proto.EventEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.Queue;

public final class DisableWriterBufferActuatorStrategy extends AbstractBinlogRecorder<WriterRequest> implements ParserStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private BufferActuator writerBufferActuator;

    @Override
    public void execute(Mapping mapping, TableGroup tableGroup, RowChangedEvent event) {
        try {
            EventEnum eventEnum = EventEnum.valueOf(event.getEvent());
            Map<String, Object> data = StringUtil.equals(ConnectorConstant.OPERTION_DELETE, eventEnum.name()) ? event.getBefore() : event.getAfter();
            BinlogMessage.Builder builder = BinlogMessage.newBuilder()
                    .setTableGroupId(tableGroup.getId())
                    .setEvent(eventEnum);
            data.forEach((k, v) -> {
                if (null != v && v instanceof String) {
                    builder.addData(Data.newBuilder().putRow(k, ByteString.copyFromUtf8((String) v)));
                }
            });
            flush(builder.build());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    protected String getTaskName() {
        return "WriterBinlog";
    }

    @Override
    protected Queue getQueue() {
        return writerBufferActuator.getQueue();
    }

    @Override
    protected WriterRequest deserialize(BinlogMessage message) {
        return null;
    }
}
package org.dbsyncer.parser.strategy.impl;

import com.google.protobuf.ByteString;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.dbsyncer.storage.binlog.AbstractBinlogRecorder;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.EventEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public final class DisableWriterBufferActuatorStrategy extends AbstractBinlogRecorder<WriterRequest> implements ParserStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private BufferActuator writerBufferActuator;

    @Autowired
    private CacheService cacheService;

    @Override
    public void execute(String tableGroupId, String event, Map<String, Object> data) {
        try {
            BinlogMap.Builder dataBuilder = BinlogMap.newBuilder();
            data.forEach((k, v) -> {
                if (null != v) {
                    ByteString bytes = serializeValue(v);
                    if (null != bytes) {
                        dataBuilder.putRow(k, bytes);
                    }
                }
            });

            BinlogMessage builder = BinlogMessage.newBuilder()
                    .setTableGroupId(tableGroupId)
                    .setEvent(EventEnum.valueOf(event))
                    .setData(dataBuilder.build())
                    .build();
            flush(builder);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void complete(List<String> messageIds) {
        super.completeMessage(messageIds);
    }

    @Override
    public Queue getQueue() {
        return writerBufferActuator.getQueue();
    }

    @Override
    public int getQueueCapacity() {
        return writerBufferActuator.getQueueCapacity();
    }

    @Override
    protected String getTaskName() {
        return "WriterBinlog";
    }

    @Override
    protected WriterRequest deserialize(String messageId, BinlogMessage message) {
        if (CollectionUtils.isEmpty(message.getData().getRowMap())) {
            return null;
        }

        // 1、获取配置信息
        final String tableGroupId = message.getTableGroupId();
        final TableGroup tableGroup = cacheService.get(tableGroupId, TableGroup.class);

        // 2、反序列数据
        final Picker picker = new Picker(tableGroup.getFieldMapping());
        final Map<String, Field> fieldMap = picker.getTargetFieldMap();
        try {
            Map<String, Object> data = new HashMap<>();
            message.getData().getRowMap().forEach((k, v) -> {
                if (fieldMap.containsKey(k)) {
                    data.put(k, resolveValue(fieldMap.get(k).getType(), v));
                }
            });
            return new WriterRequest(messageId, message.getTableGroupId(), message.getEvent().name(), data);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

}
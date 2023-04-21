package org.dbsyncer.parser;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.storage.binlog.AbstractBinlogService;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.EventEnum;
import org.dbsyncer.storage.util.BinlogMessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public abstract class AbstractWriterBinlog extends AbstractBinlogService<WriterRequest> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private BufferActuator writerBufferActuator;

    @Autowired
    private CacheService cacheService;

    protected void flush(String tableGroupId, String event, Map<String, Object> data) {
        try {
            BinlogMessage builder = BinlogMessage.newBuilder()
                    .setTableGroupId(tableGroupId)
                    .setEvent(EventEnum.valueOf(event))
                    .setData(BinlogMessageUtil.toBinlogMap(data))
                    .build();
            super.flush(builder);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    protected WriterRequest deserialize(String messageId, BinlogMessage message) {
        if (CollectionUtils.isEmpty(message.getData().getRowMap())) {
            return null;
        }

        // 1、获取配置信息
        final TableGroup tableGroup = cacheService.get(message.getTableGroupId(), TableGroup.class);
        if(tableGroup == null){
            return null;
        }

        // 2、反序列数据
        Map<String, Object> data = new HashMap<>();
        try {
            final Picker picker = new Picker(tableGroup.getFieldMapping());
            final Map<String, Field> fieldMap = picker.getSourceFieldMap();
            message.getData().getRowMap().forEach((k, v) -> {
                if (fieldMap.containsKey(k)) {
                    data.put(k, BinlogMessageUtil.deserializeValue(fieldMap.get(k).getType(), v));
                }
            });
            return new WriterRequest(messageId, message.getTableGroupId(), message.getEvent().name(), data);
        } catch (Exception e) {
            Table sTable = tableGroup.getSourceTable();
            Table tTable = tableGroup.getTargetTable();
            logger.error("messageId:{}, sTable:{}, tTable:{}, event:{}, data:{}", messageId, sTable.getName(), tTable.getName(), message.getEvent().name(), data);
            logger.error(messageId, e);
        }
        return null;
    }

    @Override
    public Queue getQueue() {
        return writerBufferActuator.getQueue();
    }

}
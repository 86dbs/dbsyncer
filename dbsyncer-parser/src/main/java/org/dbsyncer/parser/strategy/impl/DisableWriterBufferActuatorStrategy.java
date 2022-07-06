package org.dbsyncer.parser.strategy.impl;

import com.google.protobuf.ByteString;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.storage.binlog.AbstractBinlogRecorder;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.EventEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public final class DisableWriterBufferActuatorStrategy extends AbstractBinlogRecorder<WriterRequest> implements ParserStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private BufferActuator writerBufferActuator;

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private CacheService cacheService;

    @Override
    public void execute(Mapping mapping, TableGroup tableGroup, RowChangedEvent event) {
        try {
            EventEnum eventEnum = EventEnum.valueOf(event.getEvent());
            Map<String, Object> data = StringUtil.equals(ConnectorConstant.OPERTION_DELETE, eventEnum.name()) ? event.getBefore() : event.getAfter();

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
                    .setTableGroupId(tableGroup.getId())
                    .setEvent(eventEnum)
                    .setData(dataBuilder.build())
                    .build();
            flush(builder);
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
        if (CollectionUtils.isEmpty(message.getData().getRowMap())) {
            return null;
        }

        // 1、获取配置信息
        final String tableGroupId = message.getTableGroupId();
        final TableGroup tableGroup = cacheService.get(tableGroupId, TableGroup.class);
        final Mapping mapping = cacheService.get(tableGroup.getMappingId(), Mapping.class);
        final String event = message.getEvent().name();
        final String sourceTableName = tableGroup.getSourceTable().getName();
        final String targetTableName = tableGroup.getTargetTable().getName();

        // 2、反序列数据
        final Picker picker = new Picker(tableGroup.getFieldMapping());
        final Map<String, Field> fieldMap = picker.getSourceFieldMap();
        Map<String, Object> data = new HashMap<>();
        message.getData().getRowMap().forEach((k, v) -> {
            if (fieldMap.containsKey(k)) {
                data.put(k, resolveValue(fieldMap.get(k).getType(), v));
            }
        });

        // 3、获取目标源数据集合
        Map target = picker.pickData(data);

        // 4、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), target);

        // 5、插件转换
        pluginFactory.convert(tableGroup.getPlugin(), event, data, target);

        return new WriterRequest(tableGroupId, target, mapping.getMetaId(), mapping.getTargetConnectorId(), sourceTableName, targetTableName, event, picker.getTargetFields(), tableGroup.getCommand());
    }

}
package org.dbsyncer.parser.strategy.impl;

import com.google.protobuf.ByteString;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.storage.binlog.AbstractBinlogRecorder;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.Data;
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
        String tableGroupId = message.getTableGroupId();
        TableGroup tableGroup = cacheService.get(tableGroupId, TableGroup.class);
        Mapping mapping = cacheService.get(tableGroup.getMappingId(), Mapping.class);

        // 1、获取映射字段
        String event = message.getEvent().name();
        String sourceTableName = tableGroup.getSourceTable().getName();
        String targetTableName = tableGroup.getTargetTable().getName();

        Map<String, Object> data = new HashMap<>();
        Picker picker = new Picker(tableGroup.getFieldMapping());
        Map target = picker.pickData(data);

        // 2、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), target);

        // 3、插件转换
        pluginFactory.convert(tableGroup.getPlugin(), event, data, target);

        return new WriterRequest(tableGroupId, target, mapping.getMetaId(), mapping.getTargetConnectorId(), sourceTableName, targetTableName, event, picker.getTargetFields(), tableGroup.getCommand());
    }
}
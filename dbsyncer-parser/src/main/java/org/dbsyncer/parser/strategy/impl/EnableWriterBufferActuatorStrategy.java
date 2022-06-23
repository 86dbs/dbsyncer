package org.dbsyncer.parser.strategy.impl;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConditionalOnProperty(value = "dbsyncer.parser.writer.buffer.actuator.enabled", havingValue = "true")
public final class EnableWriterBufferActuatorStrategy implements ParserStrategy {

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private BufferActuator writerBufferActuator;

    @Override
    public void execute(Mapping mapping, TableGroup tableGroup, RowChangedEvent event) {
        // 1、获取映射字段
        final String eventName = event.getEvent();
        Map<String, Object> data = StringUtil.equals(ConnectorConstant.OPERTION_DELETE, eventName) ? event.getBefore() : event.getAfter();
        Picker picker = new Picker(tableGroup.getFieldMapping());
        Map target = picker.pickData(data);

        // 2、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), target);

        // 3、插件转换
        pluginFactory.convert(tableGroup.getPlugin(), eventName, data, target);

        // 4、写入缓冲执行器
        writerBufferActuator.offer(new WriterRequest(tableGroup.getId(), target, mapping.getMetaId(), mapping.getTargetConnectorId(), event.getSourceTableName(), event.getTargetTableName(), eventName, picker.getTargetFields(), tableGroup.getCommand()));
    }

}
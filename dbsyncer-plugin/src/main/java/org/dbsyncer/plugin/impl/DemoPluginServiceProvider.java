/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.plugin.impl;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.FieldMapping;
import org.dbsyncer.sdk.plugin.PluginCallback;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.PluginService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public final class DemoPluginServiceProvider implements PluginService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private AppConfig appConfig;





    @Override
    public void postProcessBefore(PluginContext context) {
        logger.info("插件正在处理同步，目标源表:{}", context.getTargetTable().getName());
    }

    @Override
    public void convert(PluginContext context) {





        List<Map> sourceList = context.getSourceList();
        List<Map> targetList = context.getTargetList();

        String mappingId = context.getMappingId();

        List<Field> targetField = context.getTargetFields().stream().filter(field -> field.getTypeName().equals(DataTypeEnum.RELTABLE.name())).collect(Collectors.toList());

        targetField.forEach(field -> {
            PluginCallback pluginCallback = context.getPluginCallback();

            String tableGroupId = field.getName();
            List<FieldMapping> fieldMappings = context.getFieldMappings();
            FieldMapping fieldMapping = fieldMappings.stream().filter(s -> s.getTarget().getName().equals(tableGroupId)).collect(Collectors.toList()).get(0);
            String name = fieldMapping.getSource().getName();





//            pluginCallback.onDataProcessed(mappingId,tableGroupId,);
        });

        List<Field> childMapping = context.getSourceTable().getColumn().stream().filter(col -> col.getTypeName().equals(DataTypeEnum.RELTABLE.name())).collect(Collectors.toList());

        for (Field field : childMapping) {

            String name = field.getName();
            Object data= sourceList.get(0).get("name");

        }




        logger.info("插件正在处理{}，数据源表:{}，目标源表:{}，事件:{}，条数:{}", context.getModelEnum().getName(), context.getSourceTable().getName(), context.getTargetTable().getName(), context.getEvent(), context.getTargetList()
                .size());
    }

    @Override
    public void postProcessAfter(PluginContext context) {
        logger.info("插件正在处理同步成功的数据，目标源表:{}，事件:{}，条数:{}", context.getTargetTable().getName(), context.getEvent(), context.getTargetList().size());
    }

    @Override
    public String getVersion() {
        return appConfig.getVersion();
    }

    public String getName() {
        return "Demo";
    }
}
package org.dbsyncer.plugin.impl;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.PluginService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class DemoPluginServiceProvider implements PluginService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private AppConfig appConfig;

    @Override
    public void convert(PluginContext context) {
        context.setTerminated(true);
        logger.info("插件正在处理{}，数据源表:{}，目标源表:{}，事件:{}，条数:{}", context.getModelEnum().getName(), context.getSourceTableName(), context.getTargetTableName(),
                context.getEvent(), context.getTargetList().size());
    }

    @Override
    public void postProcessAfter(PluginContext context) {
        logger.info("插件正在处理同步成功的数据，目标源表:{}，事件:{}，条数:{}", context.getTargetTableName(), context.getEvent(), context.getTargetList().size());
    }

    @Override
    public String getVersion() {
        return appConfig.getVersion();
    }

    public String getName() {
        return "Demo";
    }
}
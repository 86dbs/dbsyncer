package org.dbsyncer.plugin.service;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.spi.ConvertContext;
import org.dbsyncer.common.spi.ConvertService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DemoConvertServiceImpl implements ConvertService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private AppConfig appConfig;

    @Override
    public void convert(ConvertContext context) {
        context.setTerminated(true);
        logger.info("插件正在处理同步数据，数据源表:{}，目标源表:{}，事件:{}，条数:{}", context.getSourceTableName(), context.getTargetTableName(),
                context.getEvent(), context.getTargetList().size());
    }

    @Override
    public void postProcessAfter(ConvertContext context) {
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
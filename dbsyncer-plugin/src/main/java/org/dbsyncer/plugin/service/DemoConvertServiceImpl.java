package org.dbsyncer.plugin.service;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.spi.ConvertService;
import org.dbsyncer.common.spi.ProxyApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class DemoConvertServiceImpl implements ConvertService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private AppConfig appConfig;

    @Override
    public void convert(ProxyApplicationContext context, List<Map> source, List<Map> target) {
    }

    @Override
    public void convert(ProxyApplicationContext context, String event, Map source, Map target) {
        logger.info("插件正在处理同步数据，事件:{}，数据:{}", event, source);
    }

    @Override
    public String getVersion() {
        return appConfig.getVersion();
    }

    public String getName() {
        return "Demo";
    }
}

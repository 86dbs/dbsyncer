package org.dbsyncer.listener;

import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ExtractorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerEnum;
import org.dbsyncer.listener.enums.ListenerTypeEnum;
import org.dbsyncer.listener.quartz.QuartzExtractor;
import org.dbsyncer.listener.quartz.ScheduledTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Component
public class ListenerFactory implements Listener {

    @Autowired
    private ConnectorFactory connectorFactory;

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    @Override
    public AbstractExtractor getExtractor(ExtractorConfig config) throws IllegalAccessException, InstantiationException {
        final ListenerConfig listenerConfig = config.getListenerConfig();
        final ConnectorConfig connectorConfig = config.getConnectorConfig();
        final String listenerType = listenerConfig.getListenerType();
        final String connectorType = connectorConfig.getConnectorType();

        AbstractExtractor extractor = getDefaultExtractor(listenerType, connectorType, config);
        extractor.setConnectorConfig(connectorConfig);
        extractor.setListenerConfig(listenerConfig);
        extractor.setMap(config.getMap());
        return extractor;
    }

    private AbstractExtractor getDefaultExtractor(String listenerType, String connectorType, ExtractorConfig config) throws IllegalAccessException, InstantiationException {
        // 默认定时抽取
        if (ListenerTypeEnum.isTiming(listenerType)) {
            Class<QuartzExtractor> clazz = (Class<QuartzExtractor>) ListenerEnum.DEFAULT.getClazz();
            QuartzExtractor extractor = clazz.newInstance();
            extractor.setConnectorFactory(connectorFactory);
            extractor.setScheduledTaskService(scheduledTaskService);
            extractor.setTableCommandConfig(config.getTableCommandConfig());
            return extractor;
        }

        // 基于日志抽取
        Assert.isTrue(ListenerTypeEnum.isLog(listenerType), "未知的同步方式.");
        Class<AbstractExtractor> clazz = (Class<AbstractExtractor>) ListenerEnum.getExtractor(connectorType);
        return clazz.newInstance();
    }

}
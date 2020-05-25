package org.dbsyncer.listener;

import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerEnum;
import org.dbsyncer.listener.enums.ListenerTypeEnum;
import org.dbsyncer.listener.quartz.ScheduledTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

@Component
public class ListenerFactory implements Listener {

    @Autowired
    private ConnectorFactory connectorFactory;

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    @Override
    public DefaultExtractor createExtractor(ConnectorConfig connectorConfig, ListenerConfig listenerConfig, Map<String, String> map)
            throws IllegalAccessException, InstantiationException {
        String listenerType = listenerConfig.getListenerType();
        String connectorType = connectorConfig.getConnectorType();
        DefaultExtractor extractor = getDefaultExtractor(listenerType, connectorType);

        extractor.setConnectorConfig(connectorConfig);
        extractor.setListenerConfig(listenerConfig);
        extractor.setMap(map);
        return extractor;
    }

    private DefaultExtractor getDefaultExtractor(String listenerType, String connectorType)
            throws IllegalAccessException, InstantiationException {
        // 默认定时抽取
        if (ListenerTypeEnum.isTiming(listenerType)) {
            Class<DefaultExtractor> clazz = (Class<DefaultExtractor>) ListenerEnum.DEFAULT.getClazz();
            DefaultExtractor extractor = clazz.newInstance();
            extractor.setConnectorFactory(connectorFactory);
            extractor.setScheduledTaskService(scheduledTaskService);
            return extractor;
        }

        // 基于日志抽取
        Assert.isTrue(ListenerTypeEnum.isLog(listenerType), "未知的同步方式.");
        Class<DefaultExtractor> clazz = (Class<DefaultExtractor>) ListenerEnum.getExtractor(connectorType);
        return clazz.newInstance();
    }

}
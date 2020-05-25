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

import java.util.List;
import java.util.Map;

@Component
public class ListenerFactory implements Listener {

    @Autowired
    private ConnectorFactory connectorFactory;

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    @Override
    public AbstractExtractor createExtractor(ExtractorConfig config)
            throws IllegalAccessException, InstantiationException {
        ConnectorConfig connectorConfig = config.getConnectorConfig();
        ListenerConfig listenerConfig = config.getListenerConfig();

        AbstractExtractor extractor = getDefaultExtractor(listenerConfig.getListenerType(), connectorConfig.getConnectorType(),
                config.getCommands(), config.getTableNames());

        extractor.setConnectorConfig(connectorConfig);
        extractor.setListenerConfig(listenerConfig);
        extractor.setMap(config.getMap());
        return extractor;
    }

    private AbstractExtractor getDefaultExtractor(String listenerType, String connectorType, List<Map<String, String>> commands, List<String> tableNames)
            throws IllegalAccessException, InstantiationException {
        // 默认定时抽取
        if (ListenerTypeEnum.isTiming(listenerType)) {
            Class<QuartzExtractor> clazz = (Class<QuartzExtractor>) ListenerEnum.DEFAULT.getClazz();
            QuartzExtractor extractor = clazz.newInstance();
            extractor.setConnectorFactory(connectorFactory);
            extractor.setScheduledTaskService(scheduledTaskService);
            extractor.setCommands(commands);
            extractor.setTableNames(tableNames);
            return extractor;
        }

        // 基于日志抽取
        Assert.isTrue(ListenerTypeEnum.isLog(listenerType), "未知的同步方式.");
        Class<AbstractExtractor> clazz = (Class<AbstractExtractor>) ListenerEnum.getExtractor(connectorType);
        return clazz.newInstance();
    }

}
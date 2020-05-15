package org.dbsyncer.listener;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerEnum;
import org.dbsyncer.listener.enums.ListenerTypeEnum;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ListenerFactory implements Listener {

    @Override
    public DefaultExtractor createExtractor(ConnectorConfig config, ListenerConfig listenerConfig, Map<String, String> map)
            throws IllegalAccessException, InstantiationException {
        Class<DefaultExtractor> clazz = (Class<DefaultExtractor>) ListenerEnum.getExtractor(config.getConnectorType());
        DefaultExtractor extractor = clazz.newInstance();
        // log/timing
        extractor.setAction(ListenerTypeEnum.getAction(listenerConfig.getListenerType()));
        extractor.setConnectorConfig(config);
        extractor.setListenerConfig(listenerConfig);
        extractor.setMap(map);
        return extractor;
    }
}
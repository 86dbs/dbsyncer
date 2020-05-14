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
    public DefaultExtractor createExtractor(ConnectorConfig config, ListenerConfig listenerConfig, Map<String, String> map) {
        DefaultExtractor extractor = ListenerEnum.getExtractor(config.getConnectorType());
        // log/timing
        extractor.setAction(ListenerTypeEnum.getAction(listenerConfig.getListenerType()));
        extractor.setConnectorConfig(config);
        extractor.setListenerConfig(listenerConfig);
        extractor.setMap(map);
        return extractor;
    }
}
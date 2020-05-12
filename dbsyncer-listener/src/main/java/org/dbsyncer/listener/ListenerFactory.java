package org.dbsyncer.listener;


import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.enums.ListenerEnum;
import org.springframework.stereotype.Component;

@Component
public class ListenerFactory implements Listener {

    @Override
    public Extractor createExtractor(ConnectorConfig connectorConfig) {
        String type = connectorConfig.getConnectorType();
        Extractor extractor = ListenerEnum.getExtractor(type);

        return extractor;
    }
}

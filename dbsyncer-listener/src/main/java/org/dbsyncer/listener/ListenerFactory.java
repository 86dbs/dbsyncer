package org.dbsyncer.listener;


import org.dbsyncer.common.model.Task;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.springframework.stereotype.Component;

@Component
public class ListenerFactory implements Listener{

    public void execute(Task task, ListenerConfig listenerConfig, ConnectorConfig connectorConfig) {
        // extract
    }
}

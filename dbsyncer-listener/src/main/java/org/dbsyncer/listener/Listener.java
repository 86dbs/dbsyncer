package org.dbsyncer.listener;

import org.dbsyncer.common.model.Task;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;

public interface Listener {
    void execute(Task t, ListenerConfig listenerConfig, ConnectorConfig config);
}

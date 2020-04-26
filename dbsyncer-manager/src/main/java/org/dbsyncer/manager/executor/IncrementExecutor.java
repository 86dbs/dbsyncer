package org.dbsyncer.manager.executor;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.manager.Executor;
import org.springframework.stereotype.Component;

/**
 * 增量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class IncrementExecutor implements Executor {

    @Override
    public boolean start(String metaId, ListenerConfig listenerConfig, ConnectorConfig connectorConfig) {
        return true;
    }

    @Override
    public boolean shutdown(String metaId) {
        return true;
    }

}
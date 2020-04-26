package org.dbsyncer.manager;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;

/**
 * 同步任务执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 16:32
 */
public interface Executor {

    /**
     * 启动同步任务
     *
     * @param metaId
     * @param listenerConfig
     * @param connectorConfig
     */
    boolean start(String metaId, ListenerConfig listenerConfig, ConnectorConfig connectorConfig);

    /**
     * 关闭同步任务
     *
     * @param metaId
     */
    boolean shutdown(String metaId);

}
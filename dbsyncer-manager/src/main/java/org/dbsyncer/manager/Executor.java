package org.dbsyncer.manager;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;

/**
 * 监听任务执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
public interface Executor {

    /**
     * 启动监听任务
     *
     * @param metaId
     * @param listenerConfig
     * @param connectorConfig
     */
    boolean launch(String metaId, ListenerConfig listenerConfig, ConnectorConfig connectorConfig);

    /**
     * 关闭监听任务
     *
     * @param metaId
     */
    boolean close(String metaId);

    /**
     * 是否运行中
     *
     * @param metaId
     * @return
     */
    boolean isRunning(String metaId);

}

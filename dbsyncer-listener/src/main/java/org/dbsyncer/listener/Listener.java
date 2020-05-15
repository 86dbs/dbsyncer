package org.dbsyncer.listener;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;

import java.util.Map;

public interface Listener {

    /**
     * 创建抽取器
     *
     * @param connectorConfig 连接器配置
     * @param listenerConfig  监听器配置
     * @param map             增量参数
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    DefaultExtractor createExtractor(ConnectorConfig connectorConfig, ListenerConfig listenerConfig, Map<String, String> map)
            throws IllegalAccessException, InstantiationException;
}
package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;

/**
 * 连接器实例，管理连接生命周期
 *
 * @param <K> 配置
 * @param <V> 实例
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/20 23:00
 */
public interface ConnectorMapper<K, V> {

    default ConnectorConfig getOriginalConfig() {
        return (ConnectorConfig) getConfig();
    }

    K getConfig();

    V getConnection();

    void close();
}
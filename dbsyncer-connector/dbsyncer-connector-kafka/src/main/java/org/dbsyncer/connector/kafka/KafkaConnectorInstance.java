/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.util.KafkaUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * Kafka连接器实例
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public final class KafkaConnectorInstance implements ConnectorInstance<KafkaConfig, KafkaClient> {
    private KafkaConfig config;
    private KafkaClient client;

    public KafkaConnectorInstance(KafkaConfig config) {
        this.config = config;
        this.client = KafkaUtil.getConnection(config);
    }

    @Override
    public String getServiceUrl() {
        return config.getBootstrapServers();
    }

    @Override
    public KafkaConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(KafkaConfig config) {
        this.config = config;
    }

    @Override
    public KafkaClient getConnection() {
        return client;
    }

    @Override
    public void close() {
        KafkaUtil.close(client);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
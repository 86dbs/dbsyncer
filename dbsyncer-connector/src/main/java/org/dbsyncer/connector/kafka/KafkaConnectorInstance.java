package org.dbsyncer.connector.kafka;

import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.util.KafkaUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;

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
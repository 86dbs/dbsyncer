package org.dbsyncer.connector.kafka;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.KafkaConfig;

public final class KafkaConnectorMapper implements ConnectorMapper<KafkaConfig, KafkaClient> {
    private KafkaConfig config;
    private KafkaClient client;

    public KafkaConnectorMapper(KafkaConfig config, KafkaClient client) {
        this.config = config;
        this.client = client;
    }

    @Override
    public KafkaConfig getConfig() {
        return config;
    }

    @Override
    public KafkaClient getConnection() {
        return client;
    }
}
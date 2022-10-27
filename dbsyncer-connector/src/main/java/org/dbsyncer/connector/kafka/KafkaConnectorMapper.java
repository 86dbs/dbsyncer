package org.dbsyncer.connector.kafka;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.util.KafkaUtil;

public final class KafkaConnectorMapper implements ConnectorMapper<KafkaConfig, KafkaClient> {
    private KafkaConfig config;
    private KafkaClient client;

    public KafkaConnectorMapper(KafkaConfig config) {
        this.config = config;
        this.client = KafkaUtil.getConnection(config);
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

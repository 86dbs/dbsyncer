/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.util.KafkaUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka连接器实例
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public final class KafkaConnectorInstance implements ConnectorInstance<KafkaConfig, AdminClient> {
    private KafkaConfig config;
    private final AdminClient client;
    private final Map<String, KafkaProducer<String, Object>> producers = new ConcurrentHashMap<>();

    public KafkaConnectorInstance(KafkaConfig config) {
        this.config = config;
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
        props.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 30000); // 连接空闲超时
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);       // 请求超时
        props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, 100);          // 重试间隔
        props.putAll(config.getProperties());
        try {
            client = AdminClient.create(props);
            getClusterId();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            close();
        }
    }

    @Override
    public String getServiceUrl() {
        return config.getUrl();
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
    public AdminClient getConnection() {
        return client;
    }

    @Override
    public void close() {
        if (!CollectionUtils.isEmpty(producers)) {
            producers.values().forEach(KafkaProducer::close);
        }
        if (client != null) {
            client.close();
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public void send(String topic, String key, Map row) {
        KafkaProducer<String, Object> producer = producers.get(topic);
        if (producer != null) {
            producer.send(new ProducerRecord<>(key, row));
        }
    }

    public void checkProducerConfig(String properties, String topic) {
        producers.putIfAbsent(topic, KafkaUtil.createProducer(config, properties));
    }

    public String getClusterId() throws ExecutionException, InterruptedException, TimeoutException {
        return client.describeCluster().clusterId().get(3, TimeUnit.SECONDS);
    }
}
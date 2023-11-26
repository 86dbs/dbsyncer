/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.dbsyncer.connector.kafka.KafkaClient;
import org.dbsyncer.connector.kafka.config.KafkaConfig;

import java.util.Properties;

/**
 * Kafka连接器工具类
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-12-16 23:09
 */
public abstract class KafkaUtil {

    public static KafkaClient getConnection(KafkaConfig config) {

        // Consumer API
        KafkaConsumer consumer;
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", config.getBootstrapServers());
            props.put("group.id", config.getGroupId());
            props.put("enable.auto.commit", true);
            props.put("auto.commit.interval.ms", 5000);
            props.put("session.timeout.ms", config.getSessionTimeoutMs());
            props.put("max.partition.fetch.bytes", config.getMaxPartitionFetchBytes());
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", config.getDeserializer());
            consumer = new KafkaConsumer<>(props);
        }

        // Producer API
        KafkaProducer producer;
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", config.getBootstrapServers());
            props.put("buffer.memory", config.getBufferMemory());
            props.put("batch.size", config.getBatchSize());
            props.put("linger.ms", config.getLingerMs());
            props.put("acks", config.getAcks());
            props.put("retries", config.getRetries());
            props.put("max.block.ms", 60000);
            props.put("max.request.size", config.getMaxRequestSize());
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", config.getSerializer());
            producer = new KafkaProducer<>(props);
        }
        return new KafkaClient(consumer, producer);
    }

    public static void close(KafkaClient client) {
        if (null != client) {
            client.close();
        }
    }

}
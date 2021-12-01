package org.dbsyncer.connector.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.kafka.KafkaClient;

import java.util.Properties;

public abstract class KafkaUtil {

    public static KafkaClient getConnection(KafkaConfig config) {
        KafkaClient client = new KafkaClient();
        // Consumer API
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", config.getBootstrapServers());
            props.put("group.id", config.getGroupId());
            props.put("enable.auto.commit", true);
            props.put("auto.commit.interval.ms", 5000);
            props.put("session.timeout.ms", config.getSessionTimeoutMs());
            props.put("max.partition.fetch.bytes", config.getMaxPartitionFetchBytes());
            props.put("key.deserializer", config.getConsumerKeyDeserializer());
            props.put("value.deserializer", config.getConsumerValueDeserializer());
            client.setConsumer(new KafkaConsumer<>(props));
        }

        // Producer API
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
            props.put("key.serializer", config.getProducerKeySerializer());
            props.put("value.serializer", config.getProducerValueSerializer());
            client.setProducer(new KafkaProducer<>(props));
        }
        return client;
    }

    public static void close(KafkaClient client) {
        if (null != client) {
            client.close();
        }
    }

}
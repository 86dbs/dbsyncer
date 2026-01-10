/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.dbsyncer.connector.kafka.KafkaClient;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.sdk.util.PropertiesUtil;

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
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
            String consumerProperties = config.getProperties().getProperty("consumerProperties");
            props.putAll(parse(consumerProperties));
            consumer = new KafkaConsumer<>(props);
        }

        // Producer API
        KafkaProducer producer;
        {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
            String properties = config.getProperties().getProperty("producerProperties");
            props.putAll(parse(properties));
            producer = new KafkaProducer<>(props);
        }
        return new KafkaClient(consumer, producer);
    }

    public static KafkaProducer createProducer(KafkaConfig config, String properties) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
        props.putAll(parse(properties));
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer createConsumer(KafkaConfig config, String properties) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
        props.putAll(parse(properties));
        return new KafkaConsumer<>(props);
    }

    public static Properties parse(String properties) {
        return PropertiesUtil.parse(properties.replaceAll("\r\n", "&"));
    }

    public static void close(KafkaClient client) {
        if (null != client) {
            client.close();
        }
    }

}
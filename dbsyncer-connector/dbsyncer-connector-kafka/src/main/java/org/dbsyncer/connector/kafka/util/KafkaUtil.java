/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.sdk.util.PropertiesUtil;

import java.util.Collections;
import java.util.Properties;

/**
 * Kafka连接器工具类
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-12-16 23:09
 */
public abstract class KafkaUtil {

    public static final String PRODUCER_PROPERTIES = "producerProperties";
    public static final String CONSUMER_PROPERTIES = "consumerProperties";

    public static KafkaProducer<String, Object> createProducer(KafkaConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
        props.putAll(parse(config.getExtInfo().getProperty(PRODUCER_PROPERTIES)));
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, Object> createConsumer(KafkaConfig config, String topic, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.putAll(parse(config.getExtInfo().getProperty(CONSUMER_PROPERTIES)));

        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public static Properties parse(String properties) {
        properties = properties.replaceAll("\r\n", "&");
        properties = properties.replaceAll("\n", "&");
        return PropertiesUtil.parse(properties);
    }

    public static String toString(Properties properties) {
        String propertiesText = PropertiesUtil.toString(properties);
        propertiesText = propertiesText.replaceAll("&","\r\n");
        return propertiesText;
    }

}
/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
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

    public static KafkaProducer<String, Object> createProducer(KafkaConfig config, String properties) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
        props.putAll(parse(properties));
        return new KafkaProducer<>(props);
    }

    public static Properties parse(String properties) {
        return PropertiesUtil.parse(properties.replaceAll("\r\n", "&"));
    }

}
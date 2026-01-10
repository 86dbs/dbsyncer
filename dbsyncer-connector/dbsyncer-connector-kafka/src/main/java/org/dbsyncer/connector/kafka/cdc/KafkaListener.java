/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.cdc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.dbsyncer.connector.kafka.KafkaConnectorInstance;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.util.KafkaUtil;
import org.dbsyncer.sdk.listener.AbstractListener;
import org.dbsyncer.sdk.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-10 14:58
 */
public class KafkaListener extends AbstractListener<KafkaConnectorInstance> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, KafkaConsumer<String, Object>> consumers = new ConcurrentHashMap<>();

    private final int fetchSize = 1000;
    private volatile boolean connected = false;
    private Worker worker;

    @Override
    public void start() {
        connected = true;
        KafkaConnectorInstance instance = getConnectorInstance();
        KafkaConfig config = instance.getConfig();
        for (Table table : customTable) {
            Properties properties = KafkaUtil.parse(config.getProperties().getProperty("consumerProperties"));
            Properties groupId = KafkaUtil.parse(table.getExtInfo().getProperty("groupId"));
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.putAll(properties);
            consumers.putIfAbsent(table.getName(), new KafkaConsumer<>(props));
        }
        worker = new Worker();
        worker.setName("kafka-parser-" + config.getUrl() + "_" + worker.hashCode());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public void close() {
        if (connected) {
            if (null != worker && !worker.isInterrupted()) {
                worker.interrupt();
                worker = null;
            }
            connected = false;
        }
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                consumers.values().forEach(consumer -> {
                    AtomicLong count = new AtomicLong();
                    while (count.get() < fetchSize) {
                        ConsumerRecords<String, Object> records = consumer.poll(100);
                        for (ConsumerRecord<String, Object> record : records) {
                            count.incrementAndGet();
                            logger.info("收到消息：offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                        }
                    }
                });
            }
        }
    }
}
